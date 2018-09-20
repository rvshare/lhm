# Copyright (c) 2011 - 2013, SoundCloud Ltd., Rany Keddo, Tobias Bielohlawek, Tobias
# Schmidt

require File.expand_path(File.dirname(__FILE__)) + '/unit_helper'

require 'lhm/table'
require 'lhm/migration'
require 'lhm/entangler'

describe Lhm::Entangler do
  include UnitHelper

  before(:each) do
    @origin = Lhm::Table.new('origin')
    @destination = Lhm::Table.new('destination')
    @migration = Lhm::Migration.new(@origin, @destination)
    @entangler = Lhm::Entangler.new(@migration)
  end

  describe 'activation' do
    before(:each) do
      @origin.columns['info'] = { :type => 'varchar(255)' }
      @origin.columns['tags'] = { :type => 'varchar(255)' }

      @destination.columns['info'] = { :type => 'varchar(255)' }
      @destination.columns['tags'] = { :type => 'varchar(255)' }
    end

    it 'should create insert trigger to destination table' do
      ddl = %Q{
        create trigger `lhmt_ins_origin`
        after insert on `origin` for each row
        replace into `destination` (`info`, `tags`) /* large hadron migration */
        values (`NEW`.`info`, `NEW`.`tags`)
      }

      @entangler.entangle.must_include strip(ddl)
    end

    it 'should create an update trigger to the destination table' do
      ddl = %Q{
        create trigger `lhmt_upd_origin`
        after update on `origin` for each row
        replace into `destination` (`info`, `tags`) /* large hadron migration */
        values (`NEW`.`info`, `NEW`.`tags`)
      }

      @entangler.entangle.must_include strip(ddl)
    end

    it 'should create a delete trigger to the destination table' do
      ddl = %Q{
        create trigger `lhmt_del_origin`
        after delete on `origin` for each row
        delete ignore from `destination` /* large hadron migration */
        where `destination`.`id` = OLD.`id`
      }

      @entangler.entangle.must_include strip(ddl)
    end

    it 'should retry trigger creation when it hits a lock wait timeout' do
      connection = mock()
      tries = 1
      @entangler = Lhm::Entangler.new(@migration, connection, retriable: {base_interval: 0, tries: tries})
      connection.expects(:execute).times(tries).raises(Mysql2::Error, 'Lock wait timeout exceeded; try restarting transaction')

      assert_raises(Mysql2::Error) { @entangler.before }
    end

    it 'should not retry trigger creation with other mysql errors' do
      connection = mock()
      connection.expects(:execute).once.raises(Mysql2::Error, 'The MySQL server is running with the --read-only option so it cannot execute this statement.')

      @entangler = Lhm::Entangler.new(@migration, connection, retriable: {base_interval: 0})
      assert_raises(Mysql2::Error) { @entangler.before }
    end

    it 'should succesfully finish after retrying' do
      connection = mock()
      connection.stubs(:execute).raises(Mysql2::Error, 'Lock wait timeout exceeded; try restarting transaction').then.returns(true)
      @entangler = Lhm::Entangler.new(@migration, connection, retriable: {base_interval: 0})

      assert @entangler.before
    end

    it 'should retry as many times as specified by configuration' do
      connection = mock()
      connection.expects(:execute).times(5).raises(Mysql2::Error, 'Lock wait timeout exceeded; try restarting transaction')
      @entangler = Lhm::Entangler.new(@migration, connection, retriable: {tries: 5, base_interval: 0})

      assert_raises(Mysql2::Error) { @entangler.before }
    end

    describe 'super long table names' do
      before(:each) do
        @origin = Lhm::Table.new('a' * 64)
        @destination = Lhm::Table.new('b' * 64)
        @migration = Lhm::Migration.new(@origin, @destination)
        @entangler = Lhm::Entangler.new(@migration)
      end

      it 'should use truncated names' do
        @entangler.trigger(:ins).length.must_be :<=, 64
        @entangler.trigger(:upd).length.must_be :<=, 64
        @entangler.trigger(:del).length.must_be :<=, 64
      end
    end
  end

  describe 'removal' do
    it 'should remove insert trigger' do
      @entangler.untangle.must_include('drop trigger if exists `lhmt_ins_origin`')
    end

    it 'should remove update trigger' do
      @entangler.untangle.must_include('drop trigger if exists `lhmt_upd_origin`')
    end

    it 'should remove delete trigger' do
      @entangler.untangle.must_include('drop trigger if exists `lhmt_del_origin`')
    end
  end
end
