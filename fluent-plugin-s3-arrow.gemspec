lib = File.expand_path("../lib", __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)

Gem::Specification.new do |spec|
  spec.name    = "fluent-plugin-s3-arrow"
  spec.version = "0.1.0"
  spec.authors = ["kanga333"]
  spec.email   = ["e411z7t40w@gmail.com"]

  spec.summary       = %q{Extends the fluent-plugin-s3 (de)compression algorithm to enable red-arrow compression.}
  spec.description   = %q{Extends the fluent-plugin-s3 (de)compression algorithm to enable red-arrow compression.}
  spec.homepage      = "https://github.com/red-data-tools/fluent-plugin-s3-arrow"
  spec.license       = "Apache-2.0"

  test_files, files  = `git ls-files -z`.split("\x0").partition do |f|
    f.match(%r{^(test|spec|features)/})
  end
  spec.files         = files
  spec.executables   = files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = test_files
  spec.require_paths = ["lib"]

  spec.add_development_dependency "benchmark-driver"
  spec.add_development_dependency "faker"
  spec.add_development_dependency "rake", "~> 12.0"
  spec.add_development_dependency "test-unit", "~> 3.0"
  spec.add_runtime_dependency "fluentd", [">= 0.14.10", "< 2"]
  spec.add_runtime_dependency "fluent-plugin-s3", ">= 1.0"
  spec.add_runtime_dependency "red-arrow", ">= 1.0"
  spec.add_runtime_dependency "red-parquet", ">= 1.0"
end
