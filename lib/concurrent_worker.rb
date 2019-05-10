require "concurrent_worker/version"

module ConcurrentWorker
  class Error < StandardError; end

  require 'thread'

  class RequestCounter
    def initialize
      @count = Queue.new
      @com = Queue.new
    end
    def push(args)
      @count.push(args)
    end
    def pop
      Thread.handle_interrupt(Object => :never) do
        @count.pop
        @com.push(true)
      end
    end
    
    def wait_until_less_than(n)
      return if @count.size < n
      while @com.pop
        break if @count.size < n
      end
    end
    def size
      @count.size
    end

    def close
      @count.close
    end

    def closed?
      @count.closed?
    end

    def rest
      result = []
      until @count.empty?
        req = @count.pop
        next if req == []
        result.push(req)
      end
      result
    end
    
  end

  

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

    attr_reader :req_counter, :snd_queue_max

    def queue_closed?
      @req_counter.closed?
    end
    def queue_empty?
      !queue_closed? && @req_counter.size == 0
    end
    def queue_available?
      !queue_closed? && @req_counter.size < @snd_queue_max
    end
    
    def initialize(*args, **options, &work_block)
      @args = args
      @options = options
      set_block(:work_block, &work_block) if work_block
      
      @state = :idle
      @result_callbacks = []
      @retired_callbacks = []

      @snd_queue_max = @options[:snd_queue_max] || 2
      @req_counter = RequestCounter.new
      @options[ :result_callback_interrupt ]  ||= :immediate 
      @options[ :retired_callback_interrupt ] ||= :immediate 

      case @options[:type]
      when :process
        class << self
          include ConcurrentProcess
        end
      when :thread
        class << self
          include ConcurrentThread
        end
      else
        class << self
          include ConcurrentThread
        end
      end
    end

    def add_callback(&callback)
      raise "block is nil" unless callback
      @result_callbacks.push(callback)
    end
    def clear_callbacks
      @result_callbacks.clear
    end
    
    def call_result_callbacks(args)
      Thread.handle_interrupt(Object => :never) do        
        Thread.handle_interrupt(Object => @options[ :result_callback_interrupt ] ) do
          @result_callbacks.each do |callback|
            callback.call(*args)
          end
        end
        @req_counter.pop
      end
    end
    
    def add_retired_callback(&callback)
      raise "block is nil" unless callback
      @retired_callbacks.push(callback)
    end
    def clear_retired_callbacks
      @retired_callbacks.clear
    end
    
    def call_retired_callbacks
      Thread.handle_interrupt(Object => @options[ :retired_callback_interrupt ] ) do
        @retired_callbacks.each do |callback|
          callback.call
        end
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

    def set_default_loop_block
      set_block(:loop_block) do
        loop do
          break if (req = receive_req).empty?
          (args, work_block) = req
          if work_block
            set_block(:work_block, &work_block)
          end
          send_res(yield_work_block(*args))
        end
      end
    end

    def set_default_base_block
      set_block(:base_block) do
        yield_loop_block
      end
    end
    
    def run
      @state = :run
      set_default_loop_block unless defined?(@loop_block) && @loop_block
      set_default_base_block unless defined?(@base_block) && @base_block
      cncr_block
    end
    
    def req(*args, &work_block)
      unless @state == :run
        run
      end
      @req_counter.wait_until_less_than(@snd_queue_max)
      begin 
        @req_counter.push([args, work_block])
        send_req([args, work_block])
        true
      rescue ClosedQueueError, IOError
        false
      end
    end
    
    def quit
      begin 
        send_req([])
        true
      rescue ClosedQueueError, IOError
        false
      end
    end
    
    def join
      @req_counter.wait_until_less_than(1)
      quit
      wait_cncr_proc
    end
  end


  module ConcurrentThread
    def cncr_block
      @thread_channel = Queue.new
      @thread =  Thread.new do
        Thread.handle_interrupt(Object => :never) do
          begin
            Thread.handle_interrupt(Object => :immediate) do
              yield_base_block
            end
          ensure
            @req_counter.close
            @thread_channel.close
            call_retired_callbacks
          end
        end
      end
    end
    
    def send_req(args)
      @thread_channel.push(args)
    end
    
    def receive_req
      @thread_channel.pop
    end

    def send_res(args)
      call_result_callbacks(args)
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
          Thread.handle_interrupt(Object => :never) do
            data = Marshal.dump(obj)
            @wio.write([data.size].pack("I"))
            @wio.write(data)
          end
        rescue Errno::EPIPE
        end
      end

      def recv
        begin
          Thread.handle_interrupt(Object => :on_blocking) do
            szdata = @rio.read(4)
            return [] if szdata.nil?
            size = szdata.unpack("I")[0]
            Marshal.load(@rio.read(size))
          end
        rescue IOError
          raise StopIteration
        end
      end
      
      def close
        [@wio, @rio].map(&:close)
      end
    end

    def set_rcv_thread
      Thread.new do
        Thread.handle_interrupt(Object => :never) do
          begin
            Thread.handle_interrupt(Object => :immediate) do
              loop do
                result = @ipc_channel.recv
                break if result == :worker_loop_finished
                raise result if result.kind_of?(Exception)

                call_result_callbacks(result)
              end
            end
          rescue
            Thread.pass
            raise $!
          ensure
            @req_counter.close
            @ipc_channel.close
            call_retired_callbacks
          end
        end
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
        end
      end
      @ipc_channel.choose_io
      @rcv_thread = set_rcv_thread
    end

    def send_req(args)
      #called from main process only
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
      @rcv_thread && @rcv_thread.join
    end
  end



  class WorkerPool < Array

    def need_new_worker?
      self.size < @max_num && self.select{ |w| w.queue_empty? }.empty?
    end
    
    def set_snd_thread
      Thread.new do
        loop do
          break if (req = @snd_queue.pop).empty?
          loop do
            delete_if{ |w| w.queue_closed? }
            if need_new_worker?
              w = deploy_worker
              w.snd_queue_max.times do
                @ready_queue.push(w)
              end
            end
            break if @ready_queue.pop.req(*req[0], &req[1])
          end
        end
      end
    end
    
    def set_rcv_thread
      Thread.new do
        loop do
          break if (result = @rcv_queue.pop).empty?
          @result_callbacks.each do |callback|
            callback.call(*result)
          end
          @req_counter.pop
        end
      end
    end

    def initialize(*args, **options, &work_block)
      @args = args
      
      @options = options
      @max_num = @options[:pool_max] || 8
      @set_blocks = []
      if work_block
        @set_blocks.push([:work_block, work_block])
      end

      @ready_queue = Queue.new
      
      @result_callbacks = []
      
      @snd_queue_max = @options[:snd_queue_max]||2
      @req_counter = RequestCounter.new

      @snd_queue = Queue.new
      @snd_thread = set_snd_thread
      
      @rcv_queue = Queue.new
      @rcv_thread = set_rcv_thread
    end
    
    def add_callback(&callback)
      raise "block is nil" unless callback
      @result_callbacks.push(callback)
    end

    def add_finished_callback(&callback)
      raise "block is nil" unless callback
      @finished_callbacks.push(callback)
    end
    

    def deploy_worker
      defined?(@work_block) || @work_block = nil
      worker_options = {
        type:                       @options[:type],
        snd_queue_max:              @snd_queue_max,
        result_callback_interrupt:  :never,
        retired_callback_interrupt: :never
      }
      w = Worker.new(*@args, worker_options, &@work_block)
      w.add_callback do |*arg|
        @rcv_queue.push(arg)
        @ready_queue.push(w)
      end

      w.add_retired_callback do
        w.req_counter.rest.each do
          |req|
          @snd_queue.push(req)
        end
        @ready_queue.push(w)
      end
      
      @set_blocks.each do |symbol, block|
        w.set_block(symbol, &block)
      end
      w.run
      self.push(w)
      w
    end

    def set_block(symbol, &block)
      @set_blocks.push([symbol, block])
    end
    
    def req(*args, &work_block)
      @req_counter.wait_until_less_than(@max_num * @snd_queue_max)
      @req_counter.push(true)
      @snd_queue.push([args, work_block])
    end

    def join
      @req_counter.wait_until_less_than(1)
      self.shift.join until self.empty?
      @rcv_queue.push([])
      @rcv_thread.join
      @snd_queue.push([])
      @snd_thread.join
    end
  end

end
