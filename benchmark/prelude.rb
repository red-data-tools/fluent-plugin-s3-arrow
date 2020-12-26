$LOAD_PATH.unshift(File.expand_path("lib"))
require "fluent/test"
require "fluent/test/driver/output"
require "fluent/test/helpers"
require "fluent/plugin/out_s3"
require "fluent/plugin/s3_compressor_arrow"
require "json"
require "faker"

GZIP_CONFIG = %[
  s3_bucket test_bucket
  store_as gzip
]

ARROW_CONFIG = %[
  s3_bucket test_bucket
  store_as arrow
  <arrow>
    format parquet
    compression gzip
    schema_from static
    <static>
      schema [
        {"name": "test_string",  "type": "string"},
        {"name": "test_uint64",  "type": "uint64"},
        {"name": "test_boolean", "type": "boolean"}
      ]
    </static>
  </arrow>
]

COLUMNIFY_CONFIG = %[
  s3_bucket test_bucket
  store_as parquet
  <compress>
    schema_type bigquery
    schema_file benchmark/schema.bq.json
    record_type jsonl
    parquet_compression_codec gzip
  </compress>
]

def create_compressor(conf = CONFIG)
  Fluent::Test::Driver::Output.new(Fluent::Plugin::S3Output) do
  end.configure(conf).instance.instance_variable_get(:@compressor)
end

state = ENV.fetch("FAKER_RANDOM_SEED", 17).to_i
Faker::Config.random = Random.new(state)

def create_chunk
    chunk = Fluent::Plugin::Buffer::MemoryChunk.new(Object.new)
    while chunk.bytesize < 8388608 do 
      data = {
          "test_string"  => Faker::Name.name,
          "test_uint64"  => Faker::Number.number(digits: 11).to_i,
          "test_boolean" => Faker::Boolean.boolean
      }
      chunk.append([data.to_json + "\n"])
    end
    return chunk
end
