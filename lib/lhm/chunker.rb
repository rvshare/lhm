# Copyright (c) 2011 - 2013, SoundCloud Ltd., Rany Keddo, Tobias Bielohlawek, Tobias
# Schmidt
require 'lhm/command'
require 'lhm/sql_helper'
require 'lhm/printer'

module Lhm
  class ChunkInsert
    def initialize(migration, lowest, highest)
      @migration = migration
      @router = Router.new(migration)
      @lowest = lowest
      @highest = highest
    end

    def sql
      "insert ignore into `#{ @router.destination_name }` (#{ @router.destination_columns }) " \
      "select #{ @router.origin_columns } from `#{ @router.origin_name }` " \
      "#{ conditions } `#{ @router.origin_name }`.`id` between #{ @lowest } and #{ @highest }"
    end

    private
    # XXX this is extremely brittle and doesn't work when filter contains more
    # than one SQL clause, e.g. "where ... group by foo". Before making any
    # more changes here, please consider either:
    #
    # 1. Letting users only specify part of defined clauses (i.e. don't allow
    # `filter` on Migrator to accept both WHERE and INNER JOIN
    # 2. Changing query building so that it uses structured data rather than
    # strings until the last possible moment.
    def conditions
      if @migration.conditions
        @migration.conditions.
          # strip ending paren
          sub(/\)\Z/, '').
          # put any where conditions in parens
          sub(/where\s(\w.*)\Z/, 'where (\\1)') + ' and'
      else
        'where'
      end
    end
  end

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
    attr_accessor :start, :limit

    def initialize(migration, connection = nil, options = {})
      @migration = migration
      @connection = connection
      @router = Router.new(migration)
      @start = options[:start] || select_start
      @limit = options[:limit] || select_limit
    end

    def validate
      if @start > @limit
        raise ArgumentErrorerror, "impossible chunk options (limit (#{@limit.inspect} must be greater than start (#{@start.inspect})"
      end
    end

    private

    def select_start
      start = @connection.select_value("select min(id) from `#{ @router.origin_name }`")
      start ? start.to_i : nil
    end

    def select_limit
      limit = @connection.select_value("select max(id) from `#{ @router.origin_name }`")
      limit ? limit.to_i : nil
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
        affected_rows = @connection.update(ChunkInsert.new(@migration, bottom, top).sql)
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
