module Lhm
  class ChunkInsert
    def initialize(migration, lowest, highest)
      @migration = migration
      @router = Router.new(migration)
      @lowest = lowest
      @highest = highest
    end

    def insert_and_return_count_of_rows_created(connection)
      connection.update(sql)
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
end
