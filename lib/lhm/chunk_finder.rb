module Lhm
  class ChunkFinder
    def initialize(migration, connection = nil, options = {})
      @migration = migration
      @connection = connection
      @router = Router.new(migration)
      @start = options[:start] || select_start_from_db
      @limit = options[:limit] || select_limit_from_db
    end

    attr_accessor :start, :limit

    def validate
      # We only validate if we have a start and a limit.
      # The absence of a start and a limit imply an empty table.
      if start && limit && start > limit
        raise ArgumentError, "impossible chunk options (limit (#{limit.inspect} must be greater than start (#{start.inspect})"
      end
    end

    private

    def select_start_from_db
      @connection.select_value("select min(id) from `#{ @router.origin_name }`")
    end

    def select_limit_from_db
      @connection.select_value("select max(id) from `#{ @router.origin_name }`")
    end
  end
end
