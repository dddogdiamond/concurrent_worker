require "concurrent_worker/version"
require 'colorize'

module ConcurrentWorker
  class Error < StandardError; end

  require 'thread'
  class Worker
    attr_accessor :channel
    # Worker               : worker class
    #  +cncr_block         : concurrent processing block : thread(as ConcurrentThread)/process(as ConcurrentProcess)
    #    +base_block       : user defined preparation to exec 'work block'
    #      +loop_block     : loop of receiving request and exec 'work block'
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
      @result_callbacks = []
      @retired_callbacks = []

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
      @result_callbacks.push(callback)
    end
    
    def call_result_callbacks(args)
      @result_callbacks.each do |callback|
        callback.call(args)
      end
    end
    
    def add_retired_callback(&callback)
      raise "block is nil" unless callback
      @retired_callbacks.push(callback)
    end
    
    def call_retired_callbacks
      @retired_callbacks.each do |callback|
        callback.call
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
        begin
          yield_base_block
        ensure
          @req_queue.close
          @thread_channel.close
          call_retired_callbacks
        end
      end
    end
    
    def send_req(args)
      begin
        @req_queue.push(args)
        @thread_channel.push(args)
        true
      rescue ClosedQueueError
        false
      end
    end
    
    def receive_req
      @thread_channel.pop
    end

    def send_res(args)
      call_result_callbacks(args)
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
        szdata = @rio.read(4)
        return [] if szdata.nil?
        size = szdata.unpack("I")[0]
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
        begin
          yield_base_block
        rescue
          @ipc_channel.send($!)
        ensure
          @ipc_channel.send(:worker_loop_finished)
          @ipc_channel.close
        end
      end
      @ipc_channel.choose_io

      @thread = Thread.new do
        begin
          loop do
            result = @ipc_channel.recv
            break if result == :worker_loop_finished
            raise result if result.kind_of?(Exception)

            call_result_callbacks(result)
            @req_queue.pop
          end
        ensure
          @ipc_channel.close
          @req_queue.close
          call_retired_callbacks
        end
      end
    end

    def send_req(args)
      begin 
        #called from main process only
        @req_queue.push(args)
        @ipc_channel.send(args)
        true
      rescue ClosedQueueError
        false
      end
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
      $stdout.flush
      begin
        Process.waitpid(@c_pid)
      rescue Errno::ECHILD
      end
      @thread && @thread.join
      $stdout.flush
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
      
      @array_mutex = Mutex.new
      @ready_queue = Queue.new
      
      @result_callbacks = []
      @callback_queue = Queue.new
      @callback_thread = Thread.new do
        loop do
          break if (result = @callback_queue.pop).empty?
          @result_callbacks.each do |callback|
            callback.call(result[0])
          end
        end
      end
    end
    
    def push( arg )
      @array_mutex.synchronize do
        super( arg )
      end
    end
    
    def shift
      @array_mutex.synchronize do
        super
      end
    end
    
    def size
      @array_mutex.synchronize do
        super
      end
    end
    
    def n_available
      @array_mutex.synchronize do
        super
      end
    end
    
    def empty?
      @array_mutex.synchronize do
        super
      end
    end
    
    def add_callback(&callback)
      raise "block is nil" unless callback
      @result_callbacks.push(callback)
    end


    def deploy_worker
      w = Worker.new(*@args, type: @options[:type], &@work_block)
      w.add_callback do |arg|
        @callback_queue.push([arg])
        @ready_queue.push(w)
      end

      if @options[:respawn]
        w.add_retired_callback do
          undone_reqs = []
          while req = w.req_queue.pop
            next if req == []
            undone_reqs.push req
          end
          
          unless undone_reqs.empty?
            new_w = deploy_worker
            undone_reqs.each do |req|
              new_w.req( *req[0], &req[1] )
            end
            self.delete w
          end
        end
      end
      
      @set_blocks.each do |symbol, block|
        w.set_block(symbol, &block)
      end
      w.run
      self.push w
      w
    end

    def set_block(symbol, &block)
      @set_blocks.push([symbol, block])
    end
    
    def req(*args, &work_block)
      if self.size < @max_num && select{ |w| w.req_queue.size == 0 }.empty?
        w = deploy_worker
        w.req_queue_max.times do
          @ready_queue.push(w)
        end
      end
      
      while !@ready_queue.pop.req(*args, &work_block)
      end
    end

    def join
      puts size
      self.shift.join until self.empty?
      puts size
      @callback_queue.push([])
      @callback_thread.join
    end
  end

end
