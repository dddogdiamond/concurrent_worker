# ConcurrentWorker
[![Build Status](https://travis-ci.org/dddogdiamond/concurrent_worker.svg?branch=master)](https://travis-ci.org/dddogdiamond/concurrent_worker)
[![Code Climate](https://codeclimate.com/github/dddogdiamond/concurrent_worker/badges/gpa.svg)](https://codeclimate.com/github/dddogdiamond/concurrent_worker)
[![Gem Version](https://badge.fury.io/rb/concurrent_worker.svg)](http://badge.fury.io/rb/concurrent_worker)
[![MIT License](http://img.shields.io/github/license/dddogdiamond/concurrent_worker.svg)](LICENSE.txt)

Ruby concurrent worker in thread/process with preparation mechanism.

## Installation

Add this line to your application's Gemfile.

```ruby
gem 'concurrent_worker'
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install concurrent_worker

## Usage

### Worker

You can define a worker setting a work block executed in other thread, and send requests to the worker.

```ruby
require 'concurrent_worker'
Thread.abort_on_exception = true

# define a work block.
logger = ConcurrentWorker::Worker.new do |*args|
  printf(*args)
  $stdout.flush
  nil
end

...
# call work block asynchronously with 'req' method.
logger.req("thread%d n=%d\n",0, 1)

logger.join
```

If you need some preparation for the work block, you can define 'base block'.

```ruby
logger = ConcurrentWorker::Worker.new do |*args|
  # work block and base block can share object with instance variable(@xxx).
  printf(@file, *args)
  @file.flush
  nil
end

# define base block for some preparation of work block.
logger.set_block(:base_block) do
  open("log.txt", "w") do |file|
    @file = file
    # 'yield_loop_block' must be called in base block.
    # work block will be called in this call.
    yield_loop_block 
  end
end
...
```

The work block and base block are executed in a same thread, and in a worker's instance scope so that they can share object with the worker's instance variable.

### WorkerPool
You can exec work block in some process concurrently.

```ruby
#define a pool of 8 workers with same work block, executed in other process.
wp = ConcurrentWorker::WorkerPool.new(type: :process, pool_max: 8) do |n|
  [n, n.times.inject(:+)]
end

# you can receive the result of work block with callback block.
wp.add_callback do |n, result|
  logger.req("n=%d,result=%d", n, result)
end

(10000000..10000200).each do |n|
  wp.req(n)
end

wp.join
```

Worker uses `Marshal::dump/load` to transport ruby object to other process. So, request arguments and result objects must be able to be Marshal dumped. 


## Development

After checking out the repo, run `bin/setup` to install dependencies. Then, run `rake test` to run the tests. You can also run `bin/console` for an interactive prompt that will allow you to experiment.

To install this gem onto your local machine, run `bundle exec rake install`. To release a new version, update the version number in `version.rb`, and then run `bundle exec rake release`, which will create a git tag for the version, push git commits and tags, and push the `.gem` file to [rubygems.org](https://rubygems.org).

## Contributing

Bug reports and pull requests are welcome on GitHub at [https://github.com/dddogdiamond/concurrent_worker](https://github.com/dddogdiamond/concurrent_worker).

## License

The gem is available as open source under the terms of the [MIT License](https://opensource.org/licenses/MIT).
