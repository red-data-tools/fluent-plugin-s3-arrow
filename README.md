# fluent-plugin-s3-arrow

[![Gem Version](https://badge.fury.io/rb/fluent-plugin-s3-arrow.svg)](https://badge.fury.io/rb/fluent-plugin-s3-arrow)
[![Docs](https://img.shields.io/badge/docs-latest-blue.svg)](https://rubydoc.info/gems/fluent-plugin-s3-arrow)

Extends the [fluent-plugin-s3](https://github.com/fluent/fluent-plugin-s3) compression algorithm to enable [red-arrow](https://github.com/apache/arrow/tree/master/ruby/red-arrow) compression.

## Installation

### Requirements

- Apache Arrow GLib and Apache Parquet GLib
  - See Apache [Arrow install document](https://arrow.apache.org/install/) for details.
- [red-arrow](https://github.com/apache/arrow/tree/master/ruby/red-arrow)
- [red-parquet](https://github.com/apache/arrow/tree/master/ruby/red-parquet)

### RubyGems

```
$ gem install fluent-plugin-s3-arrow
```

### Bundler

Add following line to your Gemfile:

```ruby
gem "fluent-plugin-s3-arrow"
```

And then execute:

```
$ bundle
```

## Configuration

Example of fluent-plugin-s3-arrow configuration.

```
<match pattern>
  @type s3

  # fluent-plugin-s3 configurations ...

  <format>
    @type json # This plugin currently supports only json formatter.
  </format>

  store_as arrow
  <arrow>
    format parquet
    compression gzip

    schema_from static
    <static>
      schema [
        {"name": "test_string", "type": "string"},
        {"name": "test_uint64", "type": "uint64"}
      ]
    </static>
  </arrow>
</match>
```

### format and compression

This plugin supports multiple columnar formats and compressions by using red-arrow. Valid settings are below.

|  format  |  compression |
| ---- | ---- |
|  arrow | gzip, zstd |
|  feather | zstd |
|  parquet   | gzip, snappy, zstd |

### schema

Schema of columnar formats.
#### schema_from static

Set the schema statically.

```
schema_from static
<static>
  schema [
    {"name": "test_string", "type": "string"},
    {"name": "test_uint64", "type": "uint64"}
  ]
</static>
```

##### schema (required)

An array containing the names and types of the fields.
#### schema_from glue

Retrieve the schema from the AWS Glue Data Catalog.

```
schema_from glue
<glue>
  catalog test_catalog
  database test_db
  table test_table
</glue>
```

##### catalog

The name of the data catalog for which to retrieve the definition. The default value is the same as the [AWS API CatalogId](https://docs.aws.amazon.com/glue/latest/webapi/API_GetTable.html).

##### database

The name of the database for which to retrieve the definition.  The default value is `default`.
##### table (required)

The name of the table for which to retrieve the definition.

## License

Apache License, Version 2.0
