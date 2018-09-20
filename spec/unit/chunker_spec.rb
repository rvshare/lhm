# Copyright (c) 2011 - 2013, SoundCloud Ltd., Rany Keddo, Tobias Bielohlawek, Tobias
# Schmidt

require File.expand_path(File.dirname(__FILE__)) + '/unit_helper'

require 'lhm/table'
require 'lhm/migration'
require 'lhm/chunker'
require 'lhm/throttler'

describe Lhm::Chunker do
  include UnitHelper

  before(:each) do
    @origin = Lhm::Table.new('foo')
    @destination = Lhm::Table.new('bar')
    @migration = Lhm::Migration.new(@origin, @destination)
    @connection = mock()
    # This is a poor man's stub
    @throttler = Object.new
    def @throttler.run
      # noop
    end
    def @throttler.stride
      1
    end

    @chunker = Lhm::Chunker.new(@migration, @connection, :throttler => @throttler,
                                                         :start     => 1,
                                                         :limit     => 10)
  end

  describe '#run' do

    it 'detects the max id to use in the chunk using the stride and use it if it is lower than the limit' do
      def @throttler.stride
        5
      end

      @connection.expects(:select_value).with(regexp_matches(/where id >= 1 order by id limit 1 offset 4/)).returns(7)
      @connection.expects(:select_value).with(regexp_matches(/where id >= 8 order by id limit 1 offset 4/)).returns(21)
      @connection.expects(:update).with(regexp_matches(/between 1 and 7/)).returns(2)
      @connection.expects(:update).with(regexp_matches(/between 8 and 10/)).returns(2)

      @chunker.run
    end


    it 'chunks the result set according to the stride size' do
      def @throttler.stride
        2
      end

      @connection.expects(:select_value).with(regexp_matches(/where id >= 1 order by id limit 1 offset 1/)).returns(2)
      @connection.expects(:select_value).with(regexp_matches(/where id >= 3 order by id limit 1 offset 1/)).returns(4)
      @connection.expects(:select_value).with(regexp_matches(/where id >= 5 order by id limit 1 offset 1/)).returns(6)
      @connection.expects(:select_value).with(regexp_matches(/where id >= 7 order by id limit 1 offset 1/)).returns(8)
      @connection.expects(:select_value).with(regexp_matches(/where id >= 9 order by id limit 1 offset 1/)).returns(10)

      @connection.expects(:update).with(regexp_matches(/between 1 and 2/)).returns(2)
      @connection.expects(:update).with(regexp_matches(/between 3 and 4/)).returns(2)
      @connection.expects(:update).with(regexp_matches(/between 5 and 6/)).returns(2)
      @connection.expects(:update).with(regexp_matches(/between 7 and 8/)).returns(2)
      @connection.expects(:update).with(regexp_matches(/between 9 and 10/)).returns(2)

      @chunker.run
    end

    it 'handles stride changes during execution' do
      # roll our own stubbing
      def @throttler.stride
        @run_count ||= 0
        @run_count = @run_count + 1
        if @run_count > 1
          3
        else
          2
        end
      end

      @connection.expects(:select_value).with(regexp_matches(/where id >= 1 order by id limit 1 offset 1/)).returns(2)
      @connection.expects(:select_value).with(regexp_matches(/where id >= 3 order by id limit 1 offset 2/)).returns(5)
      @connection.expects(:select_value).with(regexp_matches(/where id >= 6 order by id limit 1 offset 2/)).returns(8)
      @connection.expects(:select_value).with(regexp_matches(/where id >= 9 order by id limit 1 offset 2/)).returns(nil)

      @connection.expects(:update).with(regexp_matches(/between 1 and 2/)).returns(2)
      @connection.expects(:update).with(regexp_matches(/between 3 and 5/)).returns(2)
      @connection.expects(:update).with(regexp_matches(/between 6 and 8/)).returns(2)
      @connection.expects(:update).with(regexp_matches(/between 9 and 10/)).returns(2)

      @chunker.run
    end

    it 'correctly copies single record tables' do
      @chunker = Lhm::Chunker.new(@migration, @connection, :throttler => @throttler,
                                                           :start     => 1,
                                                           :limit     => 1)

      @connection.expects(:select_value).with(regexp_matches(/where id >= 1 order by id limit 1 offset 0/)).returns(nil)
      @connection.expects(:update).with(regexp_matches(/between 1 and 1/)).returns(1)

      @chunker.run
    end

    it 'copies the last record of a table, even it is the start of the last chunk' do
      @chunker = Lhm::Chunker.new(@migration, @connection, :throttler => @throttler,
                                                           :start     => 2,
                                                           :limit     => 10)
      def @throttler.stride
        2
      end

      @connection.expects(:select_value).with(regexp_matches(/where id >= 2 order by id limit 1 offset 1/)).returns(3)
      @connection.expects(:select_value).with(regexp_matches(/where id >= 4 order by id limit 1 offset 1/)).returns(5)
      @connection.expects(:select_value).with(regexp_matches(/where id >= 6 order by id limit 1 offset 1/)).returns(7)
      @connection.expects(:select_value).with(regexp_matches(/where id >= 8 order by id limit 1 offset 1/)).returns(9)
      @connection.expects(:select_value).with(regexp_matches(/where id >= 10 order by id limit 1 offset 1/)).returns(nil)

      @connection.expects(:update).with(regexp_matches(/between 2 and 3/)).returns(2)
      @connection.expects(:update).with(regexp_matches(/between 4 and 5/)).returns(2)
      @connection.expects(:update).with(regexp_matches(/between 6 and 7/)).returns(2)
      @connection.expects(:update).with(regexp_matches(/between 8 and 9/)).returns(2)
      @connection.expects(:update).with(regexp_matches(/between 10 and 10/)).returns(1)

      @chunker.run
    end


    it 'separates filter conditions from chunking conditions' do
      @chunker = Lhm::Chunker.new(@migration, @connection, :throttler => @throttler,
                                                           :start     => 1,
                                                           :limit     => 2)
      def @throttler.stride
        2
      end

      @connection.expects(:select_value).with(regexp_matches(/where id >= 1 order by id limit 1 offset 1/)).returns(2)
      @connection.expects(:update).with(regexp_matches(/where \(foo.created_at > '2013-07-10' or foo.baz = 'quux'\) and `foo`/)).returns(1)

      def @migration.conditions
        "where foo.created_at > '2013-07-10' or foo.baz = 'quux'"
      end

      @chunker.run
    end

    it "doesn't mess with inner join filters" do
      @chunker = Lhm::Chunker.new(@migration, @connection, :throttler => @throttler,
                                                           :start     => 1,
                                                           :limit     => 2)

      def @throttler.stride
        2
      end

      @connection.expects(:select_value).with(regexp_matches(/where id >= 1 order by id limit 1 offset 1/)).returns(2)
      @connection.expects(:update).with(regexp_matches(/inner join bar on foo.id = bar.foo_id and/)).returns(1)

      def @migration.conditions
        'inner join bar on foo.id = bar.foo_id'
      end

      @chunker.run
    end
  end
end
