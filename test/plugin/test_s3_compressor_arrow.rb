require "helper"
require 'json'
require "fluent/plugin/out_s3"
require "fluent/plugin/s3_compressor_arrow"
require "fluent/plugin/output"

class S3OutputTest < Test::Unit::TestCase
    def setup
      Fluent::Test.setup
    end
  
    S3_CONFIG = {"s3_bucket" => "test", "store_as" => "arrow"}
    SCHEMA = config_element("static", "", {"schema" => [
      {"name": "test_string", "type": "string"},
      {"name": "test_uint64", "type": "uint64"},
    ]})
    ARROW_CONFIG = config_element("arrow", "", {"schema_from" => "static"}, [SCHEMA])
    CONFIG = config_element("ROOT", "", S3_CONFIG, [ARROW_CONFIG])

    def test_configure
      d = create_driver
      c = d.instance.instance_variable_get(:@compressor)
      assert_equal :arrow, c.ext
      assert_equal 'application/x-apache-arrow-file', c.content_type
      assert c.instance_variable_get(:@options).schema.is_a?(Arrow::Schema)
    end

    data(
      'arrow_snappy':   ['arrow', 'snappy'],
      'feather_gzip':   ['feather', 'gzip'],
      'feather_snappy': ['feather', 'snappy'],
    )
    def test_invalid_configure
      format, compression = data
      arrow_config = config_element("arrow", "", { "schema_from" => "static",
        "format" => format,
        "compression" => compression,
      }, [SCHEMA])
      config = config_element("ROOT", "", S3_CONFIG, [arrow_config])
      assert_raise Fluent::ConfigError do
        create_driver(config)
      end
    end

    def test_compress
      d = create_driver
      c = d.instance.instance_variable_get(:@compressor)

      chunk = Fluent::Plugin::Buffer::MemoryChunk.new(Object.new)
      d1 = {"test_string" => 'record1', "test_uint64" => 1}
      d2 = {"test_string" => 'record2', "test_uint64" => 2, "unexpected_field" => false}
      expected_d2 = d2.dup
      expected_d2.delete "unexpected_field"
      chunk.append([d1.to_json + "\n", d2.to_json + "\n"])
      
      Tempfile.create do |tmp|
        c.compress(chunk, tmp)
        Arrow::MemoryMappedInputStream.open(tmp.path) do |input|
          reader = Arrow::RecordBatchFileReader.new(input)
          reader.each do |record_batch|
            assert_equal([d1, expected_d2], record_batch.collect(&:to_h))
          end
        end
      end
    end

    data(gzip: "gzip", zstd: "zstd")
    def test_compress_with_compression
      arrow_config = config_element("arrow", "", { "schema_from" => "static",
        "compression" => data,
      },[SCHEMA])
      config = config_element("ROOT", "", S3_CONFIG, [arrow_config])

      d = create_driver(conf=config)
      c = d.instance.instance_variable_get(:@compressor)

      chunk = Fluent::Plugin::Buffer::MemoryChunk.new(Object.new)
      d1 = {"test_string" => 'record1', "test_uint64" => 1}
      d2 = {"test_string" => 'record2', "test_uint64" => 2}
      chunk.append([d1.to_json + "\n", d2.to_json + "\n"])
      codec = Arrow::Codec.new(data.to_sym)
      
      Tempfile.create do |tmp|
        c.compress(chunk, tmp)
        raw_input = Arrow::MemoryMappedInputStream.open(tmp.path)
        Arrow::CompressedInputStream.new(codec,raw_input) do |input|
          reader = Arrow::RecordBatchFileReader.new(input)
          reader.each do |record_batch|
            assert_equal([d1, d2], record_batch.collect(&:to_h))
          end
        end
      end
    end

    data(
      'parquet_gzip':   ['parquet', 'gzip'],
      'parquet_snappy': ['parquet', 'snappy'],
      'parquet_zstd':   ['parquet', 'zstd'],
      'feather_zstd':   ['feather', 'zstd'],
    )
    def test_compress_with_format
      format, compression = data
      arrow_config = config_element("arrow", "", { "schema_from" => "static",
        "format" => format,
        "compression" => compression,
      },[SCHEMA])
      config = config_element("ROOT", "", S3_CONFIG, [arrow_config])

      d = create_driver(conf=config)
      c = d.instance.instance_variable_get(:@compressor)

      chunk = Fluent::Plugin::Buffer::MemoryChunk.new(Object.new)
      d1 = {"test_string" => 'record1', "test_uint64" => 1}
      d2 = {"test_string" => 'record2', "test_uint64" => 2}
      chunk.append([d1.to_json + "\n", d2.to_json + "\n"])
      
      Tempfile.create do |tmp|
        c.compress(chunk, tmp)
        table = Arrow::Table.load(tmp.path, format: format.to_sym, compress: compression.to_sym)
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
