# Copyright (c) 2011 - 2013, SoundCloud Ltd., Rany Keddo, Tobias Bielohlawek, Tobias
# Schmidt

require File.expand_path(File.dirname(__FILE__)) + '/integration_helper'

require 'lhm/table'
require 'lhm/migration'
require 'lhm/atomic_switcher'

describe Lhm::AtomicSwitcher do
  include IntegrationHelper

  before(:each) { connect_master! }

  describe 'switching' do
    before(:each) do
      Thread.abort_on_exception = true
      @origin      = table_create('origin')
      @destination = table_create('destination')
      @migration   = Lhm::Migration.new(@origin, @destination)
      Lhm.logger = Logger.new('/dev/null')
      @connection.execute('SET GLOBAL innodb_lock_wait_timeout=3')
      @connection.execute('SET GLOBAL lock_wait_timeout=3')
    end

    after(:each) do
      Thread.abort_on_exception = false
    end

    it 'should retry on lock wait timeouts' do
      connection = mock()
      connection.stubs(:data_source_exists?).returns(true)
      connection.stubs(:execute).raises(ActiveRecord::StatementInvalid, 'Lock wait timeout exceeded; try restarting transaction.').then.returns(true)

      switcher = Lhm::AtomicSwitcher.new(@migration, connection)
      switcher.retry_sleep_time = 0

      assert switcher.run
    end

    it 'should give up on lock wait timeouts after MAX_RETRIES' do
      connection = mock()
      connection.stubs(:data_source_exists?).returns(true)
      connection.stubs(:execute).twice.raises(ActiveRecord::StatementInvalid, 'Lock wait timeout exceeded; try restarting transaction.')

      switcher = Lhm::AtomicSwitcher.new(@migration, connection)
      switcher.max_retries = 2
      switcher.retry_sleep_time = 0

      assert_raises(ActiveRecord::StatementInvalid) { switcher.run }
    end

    it 'should raise on non lock wait timeout exceptions' do
      switcher = Lhm::AtomicSwitcher.new(@migration, connection)
      switcher.send :define_singleton_method, :statements do
        ['SELECT', '*', 'FROM', 'nonexistent']
      end
      -> { switcher.run }.must_raise(ActiveRecord::StatementInvalid)
    end

    it 'rename origin to archive' do
      switcher = Lhm::AtomicSwitcher.new(@migration, connection)
      switcher.run

      slave do
        data_source_exists?(@origin).must_equal true
        table_read(@migration.archive_name).columns.keys.must_include 'origin'
      end
    end

    it 'rename destination to origin' do
      switcher = Lhm::AtomicSwitcher.new(@migration, connection)
      switcher.run

      slave do
        data_source_exists?(@destination).must_equal false
        table_read(@origin.name).columns.keys.must_include 'destination'
      end
    end
  end
end
