require File.expand_path(File.dirname(__FILE__)) + '/integration_helper'

require 'lhm'

describe Lhm do
  include IntegrationHelper

  before(:each) do 
    connect_master! 
    table_create(:users)

    @connection = Lhm.connection
    @connection.execute("SET GLOBAL innodb_lock_wait_timeout=10")
    @connection.execute("SET GLOBAL lock_wait_timeout=10")

    @connection.execute("SET SESSION innodb_lock_wait_timeout=1") 
    @connection.execute("SET SESSION lock_wait_timeout=1")
  end

  it "set_session_lock_wait_timeouts should set the sessions lock wait timeouts to less than the global values by a delta" do
    global_innodb_lock_wait_timeout = @connection.execute("SHOW GLOBAL VARIABLES LIKE 'innodb_lock_wait_timeout'").first.last.to_i
    global_lock_wait_timeout = @connection.execute("SHOW GLOBAL VARIABLES LIKE 'lock_wait_timeout'").first.last.to_i

    invoker = Lhm::Invoker.new(Lhm::Table.parse(:users, @connection), @connection, {})
    invoker.set_session_lock_wait_timeouts

    session_innodb_lock_wait_timeout = @connection.execute("SHOW SESSION VARIABLES LIKE 'innodb_lock_wait_timeout'").first.last.to_i
    session_lock_wait_timeout = @connection.execute("SHOW SESSION VARIABLES LIKE 'lock_wait_timeout'").first.last.to_i

    session_lock_wait_timeout.must_equal global_lock_wait_timeout + Lhm::Invoker::LOCK_WAIT_TIMEOUT_DELTA
    session_innodb_lock_wait_timeout.must_equal global_innodb_lock_wait_timeout + Lhm::Invoker::LOCK_WAIT_TIMEOUT_DELTA
  end

  it "set_session_lock_wait_timeouts should  raise if any of the global variables is smaller than the absolute value of the delta" do
    begin
      lock_wait_timeout_small_value = Lhm::Invoker::LOCK_WAIT_TIMEOUT_DELTA.abs - 1
      @connection.execute("SET GLOBAL innodb_lock_wait_timeout=#{lock_wait_timeout_small_value}") 
      @connection.execute("SET GLOBAL lock_wait_timeout=#{lock_wait_timeout_small_value}")

      invoker = Lhm::Invoker.new(Lhm::Table.parse(:users, @connection), @connection, {})
      p = Proc.new {invoker.set_session_lock_wait_timeouts}
      p.must_raise(RuntimeError)
    ensure
      @connection.execute("SET GLOBAL innodb_lock_wait_timeout=10")
      @connection.execute("SET GLOBAL lock_wait_timeout=10")
    end
  end

end
