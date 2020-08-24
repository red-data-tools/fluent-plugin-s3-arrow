require "helper"
require 'msgpack'
require "fluent/plugin/out_s3"
require "fluent/plugin/s3_compressor_arrow"
require "fluent/plugin/output"

class S3OutputTest < Test::Unit::TestCase
    def setup
      Fluent::Test.setup
    end
  
    CONFIG = %[
      s3_bucket test_bucket
      store_as arrow
      <compress>
        schema [
          {"name": "test_string", "type": "string"},
          {"name": "test_uint64", "type": "uint64"}
        ]
      </compress>
    ]

    def test_configure
      d = create_driver
      c = d.instance.instance_variable_get(:@compressor)
      assert_equal :arrow, c.ext
      assert_equal 'application/x-apache-arrow-file', c.content_type
      assert c.instance_variable_get(:@arrow_schema).is_a?(Arrow::Schema)
      assert_equal 1024, c.instance_variable_get(:@compress).arrow_chunk_size
    end

    def test_compress
      d = create_driver
      c = d.instance.instance_variable_get(:@compressor)

      chunk = Fluent::Plugin::Buffer::MemoryChunk.new(Object.new)
      d1 = {"test_string" => 'record1', "test_uint64" => 1}
      d2 = {"test_string" => 'record2', "test_uint64" => 2}
      chunk.append([d1.to_msgpack, d2.to_msgpack])
      
      Tempfile.create do |tmp|
        c.compress(chunk, tmp)
        Arrow::MemoryMappedInputStream.open(tmp.path) do |input|
          reader = Arrow::RecordBatchFileReader.new(input)
          reader.each do |record_batch|
            assert_equal([d1, d2], record_batch.collect(&:to_h))
          end
        end
      end
    end

    PARQUET_CONFIG = %[
      s3_bucket test_bucket
      store_as arrow
      <compress>
        arrow_format parquet
        schema [
          {"name": "test_string", "type": "string"},
          {"name": "test_uint64", "type": "uint64"}
        ]
      </compress>
    ]

    def test_compress_with_parquet
      d = create_driver(conf=PARQUET_CONFIG)
      c = d.instance.instance_variable_get(:@compressor)

      chunk = Fluent::Plugin::Buffer::MemoryChunk.new(Object.new)
      d1 = {"test_string" => 'record1', "test_uint64" => 1}
      d2 = {"test_string" => 'record2', "test_uint64" => 2}
      chunk.append([d1.to_msgpack, d2.to_msgpack])
      
      Tempfile.create do |tmp|
        c.compress(chunk, tmp)
        table = Arrow::Table.load(tmp.path, format: :parquet)
        table.each_record_batch do |record_batch|
          assert_equal([d1, d2], record_batch.collect(&:to_h))
        end
      end
    end

    private

    def create_driver(conf = CONFIG)
      Fluent::Test::Driver::Output.new(Fluent::Plugin::S3Output) do
      end.configure(conf)
    end
end
