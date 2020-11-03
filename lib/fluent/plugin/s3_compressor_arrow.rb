require 'arrow'
require 'parquet'

module Fluent::Plugin
  class S3Output
    class ArrowCompressor < Compressor
      S3Output.register_compressor('arrow', self)

      INVALID＿COMBINATIONS = {
        :arrow => [:snappy],
        :feather => [:gzip, :snappy],
      }

      config_section :arrow, multi: false do
        config_param :schema, :array
        config_param :format, :enum, list: [:arrow, :feather, :parquet], default: :arrow
        SUPPORTED_COMPRESSION = [:gzip, :snappy, :zstd]
        config_param :compression, :enum, list: SUPPORTED_COMPRESSION, default: nil
        config_param :chunk_size, :integer, default: nil
      end

      def configure(conf)
        super

        if INVALID＿COMBINATIONS[@arrow.format]&.include? @arrow.compression
          raise Fluent::ConfigError, "#{@arrow.format} unsupported with #{@arrow.format}"
        end

        @schema = Arrow::Schema.new(@arrow.schema)
        @options = Arrow::JSONReadOptions.new
        @options.schema = @schema
        @options.unexpected_field_behavior = :ignore
      end

      def ext
        @arrow.format.freeze
      end

      def content_type
        'application/x-apache-arrow-file'.freeze
      end

      def compress(chunk, tmp)
        buffer = Arrow::Buffer.new(chunk.read)
        stream = Arrow::BufferInputStream.new(buffer)
        table = Arrow::JSONReader.new(stream, @options)

        save_options = {
          format: @arrow.format,
          compression: @arrow.compression,
        }
        save_options[:chunk_size] = @arrow.chunk_size if @arrow.chunk_size
        
        table.read.save(tmp,save_options)
      end
    end
  end
end
