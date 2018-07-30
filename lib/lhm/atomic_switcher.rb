# Copyright (c) 2011 - 2013, SoundCloud Ltd., Rany Keddo, Tobias Bielohlawek, Tobias
# Schmidt

require 'lhm/command'
require 'lhm/migration'
require 'lhm/retry_helper'

module Lhm
  # Switches origin with destination table using an atomic rename.
  #
  # It should only be used if the MySQL server version is not affected by the
  # bin log affecting bug #39675. This can be verified using
  # Lhm::SqlHelper.supports_atomic_switch?.
  class AtomicSwitcher
    include Command

    attr_reader :connection

    DEFAULT_MAX_RETRIES = 600
    DEFAULT_RETRY_WAIT = 10
    include RetryHelper

    def initialize(migration, connection = nil, options = {})
      @migration = migration
      @connection = connection
      @origin = migration.origin
      @destination = migration.destination
      @max_retries = options[:max_retries]
      @retry_wait = options[:retry_wait]
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
      execute_with_retries(atomic_switch)
    end
  end
end
