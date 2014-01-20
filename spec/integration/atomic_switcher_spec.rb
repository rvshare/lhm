# Copyright (c) 2011 - 2013, SoundCloud Ltd., Rany Keddo, Tobias Bielohlawek, Tobias
# Schmidt

require File.expand_path(File.dirname(__FILE__)) + '/integration_helper'

require 'lhm/table'
require 'lhm/migration'
require 'lhm/atomic_switcher'


describe Lhm::AtomicSwitcher do
  include IntegrationHelper

  before(:each) { connect_master! }

  describe "switching" do
    before(:each) do
      @origin      = table_create("origin")
      @destination = table_create("destination")
      @migration   = Lhm::Migration.new(@origin, @destination, "id")
      @connection.execute("SET GLOBAL innodb_lock_wait_timeout=3")
      @connection.execute("SET GLOBAL lock_wait_timeout=3")
    end

   it "should retry on lock wait timeouts" do
     begin
       old_verbose, $VERBOSE = $VERBOSE, nil
       old_retry_sleep = Lhm::AtomicSwitcher::RETRY_SLEEP_TIME
       Lhm::AtomicSwitcher::RETRY_SLEEP_TIME = 0.2

       mutex = Mutex.new
       cond = ConditionVariable.new

       locking_thread = Thread.new do 
         with_per_thread_lhm_connection do |conn|

           conn.sql('BEGIN')
           conn.sql("DELETE from #{@destination.name}")
           mutex.synchronize { sleep(1); cond.signal }
           sleep(10)
           conn.sql('ROLLBACK')
         end
       end

       switching_thread = Thread.new do
         with_per_thread_lhm_connection do |conn|
           switcher = Lhm::AtomicSwitcher.new(@migration, conn)
           mutex.synchronize do
             cond.wait(mutex)
             switcher.run
           end
           Thread.current[:retries] = switcher.retries
         end
       end

       switching_thread.join
       assert switching_thread[:retries] > 0, "The switcher did not retry"
     ensure
       Lhm::AtomicSwitcher::RETRY_SLEEP_TIME = old_retry_sleep
       $VERBOSE = old_verbose
     end
   end

   it "should give up on lock wait timeouts after MAX_RETRIES" do
     begin
       old_verbose, $VERBOSE = $VERBOSE, nil

       old_retry_sleep = Lhm::AtomicSwitcher::RETRY_SLEEP_TIME
       old_max_retries = Lhm::AtomicSwitcher::MAX_RETRIES
       Lhm::AtomicSwitcher::RETRY_SLEEP_TIME = 0
       Lhm::AtomicSwitcher::MAX_RETRIES = 2

       mutex = Mutex.new
       cond = ConditionVariable.new

       locking_thread = Thread.new do 
         with_per_thread_lhm_connection do |conn|

           conn.sql('BEGIN')
           conn.sql("DELETE from #{@destination.name}")
           mutex.synchronize { sleep(1); cond.signal }
           sleep(100)
           conn.sql('ROLLBACK')
         end
       end

       switching_thread = Thread.new do
         with_per_thread_lhm_connection do |conn|
           switcher = Lhm::AtomicSwitcher.new(@migration, conn)
           mutex.synchronize do
             cond.wait(mutex)
             begin
               switcher.run
             rescue ActiveRecord::StatementInvalid => error
               Thread.current[:exception] = error

             end
           end
         end
       end

       switching_thread.join
       assert switching_thread[:exception].is_a?(ActiveRecord::StatementInvalid)
     ensure
       Lhm::AtomicSwitcher::RETRY_SLEEP_TIME = old_retry_sleep
       Lhm::AtomicSwitcher::MAX_RETRIES = old_max_retries
       $VERBOSE = old_verbose
     end
   end

    it "should raise on non lock wait timeout exceptions" do
      switcher = Lhm::AtomicSwitcher.new(@migration, connection)
      switcher.send :define_singleton_method, :statements do 
        ['SELECT', '*', 'FROM', 'nonexistent']
      end
      ->{ switcher.run }.must_raise(ActiveRecord::StatementInvalid)
    end

    it "rename origin to archive" do
      switcher = Lhm::AtomicSwitcher.new(@migration, connection)
      switcher.run

      slave do
        table_exists?(@origin).must_equal true
        table_read(@migration.archive_name).columns.keys.must_include "origin"
      end
    end

    it "rename destination to origin" do
      switcher = Lhm::AtomicSwitcher.new(@migration, connection)
      switcher.run

      slave do
        table_exists?(@destination).must_equal false
        table_read(@origin.name).columns.keys.must_include "destination"
      end
    end
  end
end
