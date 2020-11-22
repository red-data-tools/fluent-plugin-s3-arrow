module FluentPluginS3Arrow
  class CatalogError < RuntimeError; end
  class CatalogNotFoundError < CatalogError; end
  class CatalogLoadError < CatalogError; end

  module Catalogs
    @catalogs = {}

    def self.register(name, catalog_class)
      @catalogs[normalize_name(name)] = {
        class: catalog_class,
        prepared: false,
      }
    end

    def self.lookup(name)
      catalog = @catalogs[normalize_name(name)]
      unless catalog
        raise CatalogNotFoundError, "Catalog is not found: #{name.inspect}"
      end
      catalog_class = catalog[:class]
      unless catalog[:prepared]
        if catalog_class.respond_to?(:prepare)
          begin
            catalog_class.prepare
          rescue LoadError
            raise CatalogLoadError, "Catalog load error: #{name.inspect}"
          end
        end
        catalog[:prepared] = true
      end
      catalog_class
    end

    private_class_method def self.normalize_name(name)
      case name
      when Symbol
        name.to_s
      else
        name.to_str
      end
    end
  end
end

require "fluent-plugin-s3-arrow/catalogs/aws_glue"
