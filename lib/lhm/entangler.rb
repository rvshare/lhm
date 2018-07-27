# Copyright (c) 2011 - 2013, SoundCloud Ltd., Rany Keddo, Tobias Bielohlawek, Tobias
# Schmidt

require 'retriable'
require 'lhm/command'
require 'lhm/sql_helper'

module Lhm
  class Entangler
    include Command
    include SqlHelper

    attr_reader :connection

    LOCK_WAIT_RETRIES = 10
    RETRY_WAIT = 1

    # Creates entanglement between two tables. All creates, updates and deletes
    # to origin will be repeated on the destination table.
    def initialize(migration, connection = nil, options = {})
      @intersection = migration.intersection
      @origin = migration.origin
      @destination = migration.destination
      @connection = connection
      @max_retries = options[:lock_wait_retries] || LOCK_WAIT_RETRIES
      @sleep_duration = options[:retry_wait] || RETRY_WAIT
    end

    def entangle
      [
        create_delete_trigger,
        create_insert_trigger,
        create_update_trigger
      ]
    end

    def untangle
      [
        "drop trigger if exists `#{ trigger(:del) }`",
        "drop trigger if exists `#{ trigger(:ins) }`",
        "drop trigger if exists `#{ trigger(:upd) }`"
      ]
    end

    def create_insert_trigger
      strip %Q{
        create trigger `#{ trigger(:ins) }`
        after insert on `#{ @origin.name }` for each row
        replace into `#{ @destination.name }` (#{ @intersection.destination.joined }) #{ SqlHelper.annotation }
        values (#{ @intersection.origin.typed('NEW') })
      }
    end

    def create_update_trigger
      strip %Q{
        create trigger `#{ trigger(:upd) }`
        after update on `#{ @origin.name }` for each row
        replace into `#{ @destination.name }` (#{ @intersection.destination.joined }) #{ SqlHelper.annotation }
        values (#{ @intersection.origin.typed('NEW') })
      }
    end

    def create_delete_trigger
      strip %Q{
        create trigger `#{ trigger(:del) }`
        after delete on `#{ @origin.name }` for each row
        delete ignore from `#{ @destination.name }` #{ SqlHelper.annotation }
        where `#{ @destination.name }`.`id` = OLD.`id`
      }
    end

    def trigger(type)
      "lhmt_#{ type }_#{ @origin.name }"[0...64]
    end

    def expected_triggers
      [trigger(:ins), trigger(:upd), trigger(:del)]
    end

    def validate
      unless @connection.data_source_exists?(@origin.name)
        error("#{ @origin.name } does not exist")
      end

      unless @connection.data_source_exists?(@destination.name)
        error("#{ @destination.name } does not exist")
      end
    end

    def before
      entangle.each do |stmt|
        Retriable.retriable(retry_config) do
          @connection.execute(tagged(stmt))
        end
      end
    end

    def after
      untangle.each do |stmt|
        Retriable.retriable(retry_config) do
          @connection.execute(tagged(stmt))
        end
      end
    end

    def revert
      after
    end

    private

    def strip(sql)
      sql.strip.gsub(/\n */, "\n")
    end

    def retry_config
      {
        on: {
          StandardError => [/Lock wait timeout exceeded/]
        },
        tries: @max_retries, # number of attempts
        base_interval: @sleep_duration, # initial interval in seconds between tries
        multiplier: 1.5, # each successive interval grows by this factor
        rand_factor: 0.25, # percentage to randomize the next retry interval time
        max_elapsed_time: 900, # max total time in seconds that code is allowed to keep being retried
        on_retry: Proc.new do |exception, try, elapsed_time, next_interval|
          Lhm.logger.info("#{exception.class}: '#{exception.message}' - #{try} tries in #{elapsed_time} seconds and #{next_interval} seconds until the next try.")
        end
      }
    end
  end
end
