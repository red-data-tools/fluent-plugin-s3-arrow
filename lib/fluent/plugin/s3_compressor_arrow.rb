require 'arrow'
require 'parquet'
require 'fluent/msgpack_factory'

module Fluent::Plugin
  class S3Output
    class ArrowCompressor < Compressor
      S3Output.register_compressor('arrow', self)

      config_section :compress, multi: false do
        config_param :schema, :array
        config_param :arrow_format, :enum, list: [:arrow, :parquet], default: :arrow
        config_param :arrow_chunk_size, :integer, default: 1024
      end

      def configure(conf)
        super

        @arrow_schema = ::Arrow::Schema.new(@compress.schema)
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
          chunk_size: @compress.arrow_chunk_size)
      end
    end
  end
end
