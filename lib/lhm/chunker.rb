# Copyright (c) 2011 - 2013, SoundCloud Ltd., Rany Keddo, Tobias Bielohlawek, Tobias
# Schmidt
require 'lhm/command'
require 'lhm/sql_helper'
require 'lhm/printer'
require 'lhm/chunk_insert'

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

  class Chunker
    include Command
    include SqlHelper

    attr_reader :connection

    # Copy from origin to destination in chunks of size `stride`.
    # Use the `throttler` class to sleep between each stride.
    def initialize(migration, connection = nil, options = {})
      @migration = migration
      @connection = connection
      @router = Router.new(migration)
      @chunk_finder = ChunkFinder.new(migration, connection, options)
      if @throttler = options[:throttler]
        @throttler.connection = @connection if @throttler.respond_to?(:connection=)
      end
      @start = @chunk_finder.start
      @limit = @chunk_finder.limit
      @printer = options[:printer] || Printer::Percentage.new
    end

    def execute
      return unless @start && @limit
      @next_to_insert = @start
      while @next_to_insert <= @limit || (@start == @limit)
        stride = @throttler.stride
        top = upper_id(@next_to_insert, stride)
        affected_rows = ChunkInsert.new(@migration, bottom, top).execute(@connection)
        if @throttler && affected_rows > 0
          @throttler.run
        end
        @printer.notify(bottom, @limit)
        @next_to_insert = top + 1
        break if @start == @limit
      end
      @printer.end
    end

    private

    def bottom
      @next_to_insert
    end

    def upper_id(next_id, stride)
      top = connection.select_value("select id from `#{ @router.origin_name }` where id >= #{ next_id } order by id limit 1 offset #{ stride - 1}")
      [top ? top.to_i : @limit, @limit].min
    end

    def validate
      @chunk_finder.validate
    end
  end
end
