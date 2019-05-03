require "concurrent_worker/version"

module ConcurrentWorker
  class Error < StandardError; end

  require 'thread'
  class Worker
    attr_accessor :channel
    # Worker               : worker class
    #  +cncr_block         : concurrent processing block : thread(as ConcurrentThread)/process(as ConcurrentProcess)
    #    +base_block       : user defined preparation to exec 'work block'
    #      +loop_block     : loop of receiving request and exec 'looped block'
    #        +work_block   : user requested work
    #
    # These blocks are executed with 'instance_exec' method of worker,
    # so that they can share instance variables:@xxxx.
    #

    attr_reader :req_queue, :req_queue_max
    
    def initialize(*args, **options, &work_block)
      @args = args
      @options = options
      set_block(:work_block, &work_block) if work_block
      
      @state = :idle
      @callbacks = []

      @req_queue_max = @options[ :req_queue_max ] || 2
      @req_queue = SizedQueue.new(@req_queue_max)
      
      if @options[ :type ] == :process
        Worker.include ConcurrentProcess
      else
        Worker.include ConcurrentThread
      end
    end
    
    def add_callback(&callback)
      raise "block is nil" unless callback
      @callbacks.push(callback)
    end
    
    def call_callbacks(args)
      @callbacks.each do |callback|
        callback.call(args)
      end
    end
    
    def set_block(symbol, &block)
      raise "block is nil" unless block
      
      unless [:base_block, :loop_block, :work_block].include?(symbol)
        raise symbol.to_s + " is not used as worker block"
      end
      
      worker_block = Proc.new do |*args|
        self.instance_exec(*args, &block)
      end
      instance_variable_set("@" + symbol.to_s, worker_block)
      
      define_singleton_method("yield_" + symbol.to_s) do |*args|
        blk = instance_variable_get("@" + symbol.to_s)
        if blk
          blk.call(*args)
        else
          raise "block " + symbol.to_s + " is not defined"
        end
      end
    end
    
    def run
      @state = :run

      unless @loop_block
        set_block(:loop_block) do
          loop do
            break if (req = receive_req).empty?
            (args, work_block) = req
            if work_block
              set_block(:work_block, &work_block)
            end
            send_res(yield_work_block(args))
          end
        end
      end
      
      unless @base_block
        set_block(:base_block) do
          yield_loop_block
        end
      end
      cncr_block
    end

    
    def req(*args, &work_block)
      unless @state == :run
        run
      end
      send_req([args, work_block])
    end
    
    def quit
      send_req([])
    end
    
    def join
      quit
      wait_cncr_proc
    end
  end


  module ConcurrentThread
    def cncr_block
      @thread_channel = Queue.new
      @thread =  Thread.new do
        yield_base_block
      end
    end
    
    def send_req(args)
      @req_queue.push(args)
      @thread_channel.push(args)
    end
    
    def receive_req
      @thread_channel.pop
    end

    def send_res(args)
      call_callbacks(args)
      @req_queue.pop
    end

    def wait_cncr_proc
      @thread && @thread.join
    end
  end


  module ConcurrentProcess
    class IPCDuplexChannel
      def initialize
        @p_pid = Process.pid
        @p2c = IO.pipe('ASCII-8BIT', 'ASCII-8BIT')
        @c2p = IO.pipe('ASCII-8BIT', 'ASCII-8BIT')
      end

      def choose_io
        w_pipe, r_pipe = @p_pid == Process.pid ? [@p2c, @c2p] : [@c2p, @p2c]
        @wio, @rio = w_pipe[1], r_pipe[0]
        [w_pipe[0], r_pipe[1]].map(&:close)
      end
      
      def send(obj)
        begin
          data = Marshal.dump(obj)
          @wio.write([data.size].pack("I"))
          @wio.write(data)
        rescue Errno::EPIPE
        end
      end

      def recv
        size = @rio.read(4).unpack("I")[0]
        Marshal.load(@rio.read(size))
      end
      
      def close
        [@wio, @rio].map(&:close)
      end

    end
    
    def cncr_block
      @ipc_channel = IPCDuplexChannel.new
      @c_pid = fork do
        @ipc_channel.choose_io
        yield_base_block
        @ipc_channel.send(:worker_loop_finished)
      end
      @ipc_channel.choose_io

      @thread = Thread.new do
        loop do
          result = @ipc_channel.recv
          break if result == :worker_loop_finished
          call_callbacks(result)
          @req_queue.pop
        end
      end
    end

    def send_req(args)
      #called from main process only
      @req_queue.push(args)
      @ipc_channel.send(args)
    end
    
    def receive_req
      #called from worker process only
      @ipc_channel.recv
    end
    
    def send_res(args)
      #called from worker process only
      @ipc_channel.send(args)
    end

    def wait_cncr_proc
      begin
        Process.waitpid(@c_pid)
      rescue Errno::ECHILD
      end
      @thread && @thread.join
    end
  end



  class WorkerPool < Array
    def initialize(*args, **options, &work_block)
      @args = args
      
      @options = options
      @max_num = @options[ :pool_max ] || 8
      @set_blocks = []
      if work_block
        @set_blocks.push([:work_block, work_block])
      end
      
      @ready_queue = Queue.new
      
      @callbacks = []
      @callback_queue = Queue.new
      @callback_thread = Thread.new do
        finished_count = 0
        loop do
          break if (result = @callback_queue.pop).empty?
          @callbacks.each do |callback|
            callback.call(result[0])
          end
        end
      end
    end
    
    def add_callback(&callback)
      raise "block is nil" unless callback
      @callbacks.push(callback)
    end


    def deploy_worker
      w = Worker.new(*@args, type: @options[:type], &@work_block)
      w.add_callback do |arg|
        @callback_queue.push([arg])
        @ready_queue.push(w)
      end
      @set_blocks.each do |symbol, block|
        w.set_block(symbol, &block)
      end
      w.run
      push w
      w
    end

    def set_block(symbol, &block)
      @set_blocks.push([symbol, block])
    end
    
    def req(*args, &work_block)
      if self.size < @max_num && select{ |w| w.req_queue.size == 0 }.empty?
        w =  deploy_worker
        w.req_queue_max.times do
          @ready_queue.push(w)
        end
      end
      w = @ready_queue.pop
      w.req(*args, &work_block)
    end

    def join
      self.map(&:quit)
      self.map(&:join)
      @callback_queue.push([])
      @callback_thread.join
    end
  end

end
