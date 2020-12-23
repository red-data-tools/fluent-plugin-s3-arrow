require 'arrow'
require 'parquet'
require 'fluent-plugin-s3-arrow/schemas'

module Fluent::Plugin
  class S3Output
    class ArrowCompressor < Compressor
      S3Output.register_compressor('arrow', self)

      INVALID＿COMBINATIONS = {
        :arrow => [:snappy],
        :feather => [:gzip, :snappy],
      }

      config_section :arrow, multi: false do
        config_param :format, :enum, list: [:arrow, :feather, :parquet], default: :arrow
        SUPPORTED_COMPRESSION = [:gzip, :snappy, :zstd]
        config_param :compression, :enum, list: SUPPORTED_COMPRESSION, default: nil
        config_param :chunk_size, :integer, default: nil
        config_param :schema_from, :enum, list: [:static, :glue], default: :static

        config_section :static, multi: false do
          config_param :schema, :array, default: nil
        end

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

        @options = Arrow::JSONReadOptions.new
        @options.schema = resolve_schema
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
        case @arrow.schema_from
        when :static
          Arrow::Schema.new(@arrow.static.schema)
        when :glue
          glue_schema = FluentPluginS3Arrow::Schemas::AWSGlue.new(@arrow.glue.table, {
            catalog_id: @arrow.glue.catalog,
            database_name: @arrow.glue.database,
          })
          Arrow::Schema.new(glue_schema.to_arrow)
        end
      end
    end
  end
end
