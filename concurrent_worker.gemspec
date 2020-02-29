
lib = File.expand_path("../lib", __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require "concurrent_worker/version"

Gem::Specification.new do |spec|
  spec.name          = "concurrent_worker"
  spec.version       = ConcurrentWorker::VERSION
  spec.authors       = ["dddogdiamond"]
  spec.email         = ["dddogdiamond@gmail.com"]

  spec.summary       = %q{Concurrent worker in thread/process with preparation mechanism.}
  spec.description   = %q{Concurrent worker in thread/process with preparation mechanism.}
  spec.homepage      = "https://github.com/dddogdiamond/concurrent_worker"
  spec.license       = "MIT"


  # Specify which files should be added to the gem when it is released.
  # The `git ls-files -z` loads the files in the RubyGem that have been added into git.
  spec.files         = Dir.chdir(File.expand_path('..', __FILE__)) do
    `git ls-files -z`.split("\x0").reject { |f| f.match(%r{^(test|spec|features)/}) }
  end
  spec.bindir        = "exe"
  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  spec.add_development_dependency "bundler", "~> 2.0"
  spec.add_development_dependency "rake", "~> 13.0"
  spec.add_development_dependency "minitest", "~> 5.0"
end
