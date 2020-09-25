# fluent-plugin-s3-arrow

Extends the [fluent-plugin-s3](https://github.com/fluent/fluent-plugin-s3) (de)compression algorithm to enable red-arrow compression.

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

Example of fluent-plugin-s3 configuration.

```
<match pattern>
  @type s3

  # fluent-plugin-s3 configurations ...

  <format>
    @type json # This plugin currently supports only json formatter.
  </format>

  store_as arrow
  <arrow>
    schema [
      {"name": "test_string", "type": "string"},
      {"name": "test_uint64", "type": "uint64"}
    ]
  </arrow>
</match>
```

## License

Apache License, Version 2.0
