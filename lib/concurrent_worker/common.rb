
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
end
