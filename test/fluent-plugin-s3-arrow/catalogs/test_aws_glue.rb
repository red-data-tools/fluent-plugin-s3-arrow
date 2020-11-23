require "helper"
require 'arrow'
require 'aws-sdk-glue'
require "fluent-plugin-s3-arrow/catalogs"

class AWSGlueTest < Test::Unit::TestCase
    def setup
      stub(Aws::Glue::Client).new
      @catalog = FluentPluginS3Arrow::Catalogs.lookup("aws_glue").new()
    end

    
    def test_resolve_arrow_schema
      stub(@catalog).fetch_glue_columns{
        [
          Aws::Glue::Types::Column.new({name: "a", type: "boolean"}),
          Aws::Glue::Types::Column.new({name: "b", type: "tinyint"}),
          Aws::Glue::Types::Column.new({name: "c", type: "smallint"}),
          Aws::Glue::Types::Column.new({name: "d", type: "int"}),
          Aws::Glue::Types::Column.new({name: "e", type: "bigint"}),
          Aws::Glue::Types::Column.new({name: "f", type: "float"}),
          Aws::Glue::Types::Column.new({name: "g", type: "double"}),
          Aws::Glue::Types::Column.new({name: "h", type: "decimal(2,1)"}),
          Aws::Glue::Types::Column.new({name: "i", type: "char(1)"}),
          Aws::Glue::Types::Column.new({name: "j", type: "varchar(1)"}),
          Aws::Glue::Types::Column.new({name: "k", type: "string"}),
          Aws::Glue::Types::Column.new({name: "l", type: "binary"}),
          Aws::Glue::Types::Column.new({name: "m", type: "date"}),
          Aws::Glue::Types::Column.new({name: "n", type: "timestamp"})
        ]
      }
      actual = @catalog.resolve_arrow_schema('test')
      expect = [
        {name: "a", type: "boolean"},
        {name: "b", type: "int8"},
        {name: "c", type: "int16"},
        {name: "d", type: "int32"},
        {name: "e", type: "int64"},
        {name: "f", type: "float"},
        {name: "g", type: "double"},
        {name: "h", type: "decimal128", precision: 2, scale: 1},
        {name: "i", type: "string"},
        {name: "j", type: "string"},
        {name: "k", type: "string"},
        {name: "l", type: "binary"},
        {name: "m", type: "date32"},
        {name: "n", type: "date64"}
      ]
      
      assert_equal actual, expect
      assert_nothing_raised("Invalid arroe schema: #{actual}") { Arrow::Schema.new(actual) }
      
    end
end
