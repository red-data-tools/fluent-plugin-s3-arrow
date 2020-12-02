require 'aws-sdk-glue'

module FluentPluginS3Arrow
  module Schemas
    class AWSGlue
      class Error < RuntimeError; end
      class ConvertError < Error; end
      class Field < Struct.new(:name, :type); end

      def initialize(table_name, options={})
        @table_name = table_name
        @database_name = options[:database_name] || "default"
        @catalog_id = options[:catalog_id]
        @client = Aws::Glue::Client.new(options)
      end 

      def to_arrow()
        glue_schema = fetch_glue_schema()
        convert_to_arrow_schema(glue_schema)
      end

      private
      def fetch_glue_schema
        glue_table = @client.get_table({
          catalog_id: @catalog_id,
          database_name: @database_name,
          name: @table_name
        })
        glue_table.table.storage_descriptor.columns
      end

      def convert_to_arrow_schema(glue_schema)
        glue_schema.map do |glue_field|
          convert_to_arrow_field(glue_field)
        end
      end

      def convert_to_arrow_field(glue_field)
        arrow_field = {name: glue_field.name}
        case glue_field.type
        when "boolean", "float", "double"
          arrow_field[:type] = glue_field.type
        when "tinyint"
          arrow_field[:type] = "int8"
        when "smallint"
          arrow_field[:type] = "int16"
        when "int"
          arrow_field[:type] = "int32"
        when "bigint"
          arrow_field[:type] = "int64"
        when /\Adecimal/
          arrow_field[:type] = "decimal128"
          precision, scale = parse_decimal(glue_field.type)
          arrow_field[:precision] = precision
          arrow_field[:scale] = scale
        when /\Achar/,/\Avarchar/,"string"
          arrow_field[:type] = "string"
        when "binary"
          arrow_field[:type] = "binary"
        when "date"
          arrow_field[:type] = "date32"
        when "timestamp"
          arrow_field[:type] = "date64"
        when /\Aarray/
          arrow_field[:type] = "list"
          arrow_field[:field] = parse_array(glue_field.type)
        when /\Astruct/
          arrow_field[:type] = "struct"
          arrow_field[:fields] = parse_struct(glue_field.type)
        else
          # TODO: Need support for MAP type.
          raise ConvertError, "Input type is not supported: #{glue_field.type}"
        end
        arrow_field
      end

      def parse_decimal(str)
        matched = str.match(/\Adecimal\((\d+),(\d+)\)\z/)
        raise ConvertError, "Parse error on decimal type: #{str}" if matched.nil?
        return matched[1].to_i, matched[2].to_i
      end

      def parse_array(str)
        matched = str.match(/\Aarray<(.*)>\z/)
        raise ConvertError, "Parse error on array type: #{str}" if matched.nil?
        convert_to_arrow_field(Field.new("", matched[1]))
      end

      def parse_struct(str)
        fields = []
        matched = str.match(/\Astruct<(.*)>\z/)
        raise ConvertError, "Parse error on struct type: #{str}" if matched.nil?
        each_struct_fields(matched[1]) do |name, type|
          fields << convert_to_arrow_field(Field.new(name, type))
        end
        fields
      end

      def each_struct_fields(str)
        s, e, nest = 0, 0, 0
        name = ""
        str.each_char do |c|
          case c
          when ':'
            if nest == 0
              name = str[s..e-1]
              s = e + 1
            end
          when '<'
            nest += 1
          when '>'
            nest -= 1
          when ','
            if nest == 0
              yield(name ,str[s..e-1])
              s = e + 1
            end
          end
          e += 1
        end
        yield(name, str[s..e-1])
      end

    end
  end
end
