module Lhm
  class Router
    def initialize(migration)
      @migration = migration
    end

    def origin_name
      @origin_name ||= @migration.origin.name
    end

    def origin_columns
      @origin_columns ||= @migration.intersection.origin.typed(origin_name)
    end

    def destination_name
      @destination_name ||= @migration.destination.name
    end

    def destination_columns
      @destination_columns ||= @migration.intersection.destination.joined
    end
  end
end
