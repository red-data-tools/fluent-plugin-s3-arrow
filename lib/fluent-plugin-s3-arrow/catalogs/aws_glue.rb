module FluentPluginS3Arrow
  module Catalogs
    class AWSGlue
      class AWSGlueCatalogError < RuntimeError; end
      class AWSGlueConverteTypeError < AWSGlueCatalogError; end

      Catalogs.register(:aws_glue, self)

      class << self
        def prepare
          require 'aws-sdk-glue'
        end
      end

      def initialize(options={})
        @database_name = options[:database_name] || "default"
        # TODO: Scrutinize the necessary args.
        @client = Aws::Glue::Client.new(options)
      end 

      def resolve_arrow_schema(name)
        columns = fetch_glue_columns(name)
        convert_to_arrow_schema(columns)
      end

      private
      def fetch_glue_columns(name)
        glue_schema = @client.get_table({
          database_name: @database_name,
          name: name
        })
        glue_schema.table.storage_descriptor.columns
      end

      def convert_to_arrow_schema(columns)
        columns.map do |column|
          {name: column.name, type: convert_type(column.type)}
        end
      end

      def convert_type(glue_type)
        case glue_type
        when "boolean", "float", "double"
          glue_type
        when "tinyint"
          "int8"
        when "smallint"
          "int16"
        when "int"
          "int32"
        when "bigint"
          "int64"
        when /^decimal.*/ # TODO: Fix the inability to convert.
          glue_type.gsub(/decimal/, 'decimal128')
        when /^char.*/,/^varchar.*/,"string"
          "string"
        when "binary"
          "binary"
        when "date"
          "date32"
        when "timestamp"
          "date64"
        else
          # TODO: Need support for complex types such as ARRAY, MAP and STRUCT.
          raise AWSGlueConverteTypeError, "Input type is not supported: #{glue_type}"
        end
      end

      def convert_decimal(decimal_str)
        decimal_str.gsub(/decimal/, 'decimal128')
      end
    end
  end
end
