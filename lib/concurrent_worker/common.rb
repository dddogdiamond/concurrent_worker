
module ConcurrentWorker
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
        r = @count.pop
        @com.push(true)
        r
      end
    end
    
    def wait_until_less_than(n)
      return if @count.size < n
      while @com.pop
        break if @count.size < n
      end
    end

    def empty?
      @count.empty?
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

  end

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
        Marshal.dump(obj, @wio)
      rescue Errno::EPIPE
      end
    end

    def recv
      begin
        Marshal.load(@rio)
      rescue IOError
        raise StopIteration
      end
    end
    
    def close
      [@wio, @rio].map(&:close)
    end
  end
end
