# Copyright (c) 2011 - 2013, SoundCloud Ltd., Rany Keddo, Tobias Bielohlawek, Tobias
# Schmidt

require 'lhm/command'
require 'lhm/migration'
require 'lhm/sql_helper'

module Lhm
  # Switches origin with destination table using an atomic rename.
  #
  # It should only be used if the MySQL server version is not affected by the
  # bin log affecting bug #39675. This can be verified using
  # Lhm::SqlHelper.supports_atomic_switch?.
  class AtomicSwitcher
    include Command
    RETRY_SLEEP_TIME = 10
    MAX_RETRIES = 600

    attr_reader :connection
    attr_writer :max_retries, :retry_sleep_time

    def initialize(migration, connection = nil)
      @migration = migration
      @connection = connection
      @origin = migration.origin
      @destination = migration.destination
      @max_retries = MAX_RETRIES
      @retry_sleep_time = RETRY_SLEEP_TIME
    end

    def atomic_switch
      "rename table `#{ @origin.name }` to `#{ @migration.archive_name }`, " \
      "`#{ @destination.name }` to `#{ @origin.name }`"
    end

    def validate
      unless @connection.data_source_exists?(@origin.name) &&
             @connection.data_source_exists?(@destination.name)
        error "`#{ @origin.name }` and `#{ @destination.name }` must exist"
      end
    end

    private

    def execute
      Retriable.retriable(retry_config) do
        @connection.execute(SqlHelper.tagged(atomic_switch))
      end
    end

    def retry_config
      {
        on: {
          ActiveRecord::StatementInvalid => [/Lock wait timeout exceeded/]
        },
        tries: @max_retries, # number of attempts
        base_interval: @retry_sleep_time, # initial interval in seconds between tries
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
