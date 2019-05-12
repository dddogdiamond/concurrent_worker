module ConcurrentWorker
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
      @options[:result_callback_interrupt]  ||= :immediate 
      @options[:retired_callback_interrupt] ||= :immediate 

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
        Thread.handle_interrupt(Object => @options[:result_callback_interrupt]) do
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
      Thread.handle_interrupt(Object => @options[:retired_callback_interrupt]) do
        @retired_callbacks.each do |callback|
          callback.call
        end
      end
    end

    def result_handle_thread(&recv_block)
      Thread.new do
        Thread.handle_interrupt(Object => :never) do
          begin
            Thread.handle_interrupt(Object => :immediate) do
              recv_block.call
            end
          ensure
            @req_counter.close
            channel_close
            call_retired_callbacks
          end
        end
      end
    end


    def define_block(symbol,&block)
      worker_block = Proc.new do |*args|
        self.instance_exec(*args, &block)
      end
      instance_variable_set("@" + symbol.to_s, worker_block)
    end
    
    def define_block_yield(symbol)
      define_singleton_method("yield_" + symbol.to_s) do |*args|
        blk = instance_variable_get("@" + symbol.to_s)
        if blk
          blk.call(*args)
        else
          raise "block " + symbol.to_s + " is not defined"
        end
      end
    end
    
    def set_block(symbol, &block)
      raise "block is nil" unless block
      
      unless [:base_block, :loop_block, :work_block].include?(symbol)
        raise symbol.to_s + " is not used as worker block"
      end
      define_block(symbol,&block)
      define_block_yield(symbol)
    end

    def set_default_loop_block
      set_block(:loop_block) do
        while req = receive_req
          (args, work_block) = req
          if work_block
            set_block(:work_block, &work_block)
          end
          send_res([yield_work_block(*args)])
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
      unless @state == :run
        return
      end
      begin 
        send_req(nil)
        true
      rescue ClosedQueueError, IOError
        false
      end
    end
    
    def join
      unless @state == :run
        return
      end
      @req_counter.wait_until_less_than(1)
      quit
      wait_cncr_proc
    end
  end


  module ConcurrentThread
    def cncr_block
      @thread_channel = Queue.new
      @thread = result_handle_thread do
        yield_base_block
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

    def channel_close
      @thread_channel.close
    end

    def wait_cncr_proc
      @thread && @thread.join
    end
  end


  module ConcurrentProcess
    
    def ipc_recv_loop
      while result = @ipc_channel.recv
        raise result if result.kind_of?(Exception)

        call_result_callbacks(result)
      end
    end

    def cncr_block
      @ipc_channel = IPCDuplexChannel.new
      @c_pid = fork do
        @ipc_channel.choose_io
        Thread.handle_interrupt(Object => :never) do
          begin
            Thread.handle_interrupt(Object => :immediate) do
              yield_base_block
            end
          rescue
            @ipc_channel.send($!)
          ensure
            @ipc_channel.send(nil)
          end
        end
      end
      @ipc_channel.choose_io
      @recv_thread = result_handle_thread do
        ipc_recv_loop
      end
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

    def channel_close
      @ipc_channel.close
    end
    
    def wait_cncr_proc
      begin
        Process.waitpid(@c_pid)
      rescue Errno::ECHILD
      end
      @recv_thread && @recv_thread.join
    end
  end
end


