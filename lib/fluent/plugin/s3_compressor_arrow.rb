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
        config_param :schema, :array, default: nil
        config_param :format, :enum, list: [:arrow, :feather, :parquet], default: :arrow
        SUPPORTED_COMPRESSION = [:gzip, :snappy, :zstd]
        config_param :compression, :enum, list: SUPPORTED_COMPRESSION, default: nil
        config_param :chunk_size, :integer, default: nil
        config_param :schema_from, :enum, list: [:glue], default: nil
        config_section :glue, multi: false do
          config_param :catalog, :string, default: nil
          config_param :database, :string, default: "default"
          config_param :table, :string, default: nil
        end
      end

      def configure(conf)
        super

        if INVALID＿COMBINATIONS[@arrow.format]&.include? @arrow.compression
          raise Fluent::ConfigError, "#{@arrow.format} unsupported with #{@arrow.format}"
        end

        if !(@arrow.schema.nil? ^ @arrow.schema_from.nil?)
          raise Fluent::ConfigError, "Either schema or schema_from must be specified"
        end

        resolve_schema()
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

        table.read.save(tmp,
          format: @arrow.format,
          chunk_size: @arrow.chunk_size,
          compression: @arrow.compression,
        )
      end

      private

      def resolve_schema
        if !@arrow.schema.nil?
          @schema = Arrow::Schema.new(@arrow.schema)
          return
        end

        require 'fluent-plugin-s3-arrow/schemas'

        case @schema_from
        when :gelu
          glue_schema = FluentPluginS3Arrow::Schemas::AWSGlue.new()
          @schema = glue_schema.resolve_arrow_schema(:table, :database, :catalog)
        end
      end
    end
  end
end
