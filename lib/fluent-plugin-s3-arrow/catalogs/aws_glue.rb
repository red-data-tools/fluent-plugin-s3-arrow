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
          convert_type(column)
        end
      end

      def convert_type(column)
        arrow_column = {name: column.name}
        case column.type
        when "boolean", "float", "double"
          arrow_column[:type] = column.type
        when "tinyint"
          arrow_column[:type] = "int8"
        when "smallint"
          arrow_column[:type] = "int16"
        when "int"
          arrow_column[:type] = "int32"
        when "bigint"
          arrow_column[:type] = "int64"
        when /^decimal.*/
          arrow_column[:type] = "decimal128"
          precision, scale = parse_decimal(column.type)
          arrow_column[:precision] = precision
          arrow_column[:scale] = scale
        when /^char.*/,/^varchar.*/,"string"
          arrow_column[:type] = "string"
        when "binary"
          arrow_column[:type] = "binary"
        when "date"
          arrow_column[:type] = "date32"
        when "timestamp"
          arrow_column[:type] = "date64"
        else
          # TODO: Need support for complex types such as ARRAY, MAP and STRUCT.
          raise AWSGlueConverteTypeError, "Input type is not supported: #{column.type}"
        end
        arrow_column
      end

      def parse_decimal(decimal_str)
        /decimal\((\d+),(\d+)\)/ =~ decimal_str
        return $1.to_i, $2.to_i
      end
    end
  end
end
