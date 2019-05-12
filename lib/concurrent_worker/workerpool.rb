module ConcurrentWorker
  class WorkerPool < Array

    def need_new_worker?
      self.size < @max_num && self.select{ |w| w.queue_empty? }.empty?
    end

    def available_worker
      delete_if{ |w| w.queue_closed? }
      if need_new_worker?
        w = deploy_worker
        w.snd_queue_max.times do
          @ready_queue.push(w)
        end
      end
      @ready_queue.pop
    end
    
    def set_snd_thread
      Thread.new do
        while req = @snd_queue.pop
          (args, work_block) = req
          until available_worker.req(*args, &work_block)
          end
        end
      end
    end
    
    def set_recv_thread
      Thread.new do
        while result = @recv_queue.pop
          @result_callbacks.each do |callback|
            callback.call(*result[0])
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
      
      @recv_queue = Queue.new
      @recv_thread = set_recv_thread
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
      w.add_callback do |*args|
        @recv_queue.push([args])
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
      @recv_queue.push(nil)
      @recv_thread.join
      @snd_queue.push(nil)
      @snd_thread.join
    end
  end

end
