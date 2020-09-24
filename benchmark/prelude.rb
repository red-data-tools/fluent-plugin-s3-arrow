$LOAD_PATH.unshift(File.expand_path("lib"))
require "fluent/test"
require "fluent/test/driver/output"
require "fluent/test/helpers"
require "fluent/plugin/out_s3"
require "fluent/plugin/s3_compressor_arrow"
require "json"

GZIP_CONFIG = %[
  s3_bucket test_bucket
  store_as gzip
]

ARROW_CONFIG = %[
  s3_bucket test_bucket
  store_as arrow
  <compress>
    arrow_format arrow
    schema [
      {"name": "test_string", "type": "string"},
      {"name": "test_uint64", "type": "uint64"}
    ]
  </compress>
]

def create_compressor(conf = CONFIG)
  Fluent::Test::Driver::Output.new(Fluent::Plugin::S3Output) do
  end.configure(conf).instance.instance_variable_get(:@compressor)
end

def create_chunk
    chunk = Fluent::Plugin::Buffer::MemoryChunk.new(Object.new)
    d1 = {"test_string" => 'record1', "test_uint64" => 1}
    d2 = {"test_string" => 'record2', "test_uint64" => 2}
    while chunk.bytesize < 8388608 do
      chunk.append([d1.to_json + "\n", d2.to_json + "\n"])
    end
    return chunk
end
