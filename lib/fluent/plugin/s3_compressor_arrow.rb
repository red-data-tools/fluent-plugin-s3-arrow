require 'arrow'
require 'parquet'
require 'fluent/msgpack_factory'

module Fluent::Plugin
  class S3Output
    class ArrowCompressor < Compressor
      S3Output.register_compressor('arrow', self)

      INVALID＿COMBINATIONS = {
        :arrow => [:snappy],
        :feather => [:gzip, :snappy],
      }

      config_section :compress, multi: false do
        config_param :schema, :array
        config_param :arrow_format, :enum, list: [:arrow, :feather, :parquet], default: :arrow
        SUPPORTED_COMPRESSION = [:gzip, :snappy, :zstd]
        config_param :arrow_compression, :enum, list: SUPPORTED_COMPRESSION, default: nil
        config_param :arrow_chunk_size, :integer, default: 1024
      end

      def configure(conf)
        super

        @arrow_schema = ::Arrow::Schema.new(@compress.schema)
        if INVALID＿COMBINATIONS[@compress.arrow_format]&.include? @compress.arrow_compression
          raise Fluent::ConfigError, "#{@compress.arrow_format} unsupported with #{@compress.arrow_format}"
        end
      end

      def ext
        @compress.arrow_format.freeze
      end

      def content_type
        'application/x-apache-arrow-file'.freeze
      end

      def compress(chunk, tmp)
        msg = ::Fluent::MessagePackFactory.unpacker.feed(chunk.read)
        record_batch = ::Arrow::RecordBatch.new(@arrow_schema, msg.each.to_a)
        
        record_batch.to_table.save(tmp,
          format: @compress.arrow_format,
          chunk_size: @compress.arrow_chunk_size,
          compression: @compress.arrow_compression,
        )
      end
    end
  end
end
