require 'retriable'
require 'lhm/sql_helper'

module Lhm
  # RetryHelper standardizes the interface for retry behavior in components like
  # Entangler and AtomicSwitcher.
  #
  # To retry some behavior, use `execute_with_retries(statement)`
  # which assumes `@connection` is available.
  #
  # `execute_with_retries` expects the caller to invoke `configure_retry` first, providing:
  # * `max_retries` as an integer
  # * `retry_wait` as an integer
  module RetryHelper
    def execute_with_retries(statement)
      Retriable.retriable(retry_config) do
        @connection.execute(SqlHelper.tagged(statement))
      end
    end

    def configure_retry(options)
      @retry_config = DEFAULT_RETRY_CONFIG.merge(options)
    end

    attr_reader :retry_config

    private

    DEFAULT_RETRY_CONFIG = {
      on: {
        StandardError => [/Lock wait timeout exceeded/]
      },
      multiplier: 1.5, # each successive interval grows by this factor
      rand_factor: 0.25, # percentage to randomize the next retry interval time
      max_elapsed_time: Float::INFINITY, # max total time in seconds that code is allowed to keep being retried
      on_retry: Proc.new do |exception, try, elapsed_time, next_interval|
        Lhm.logger.info("#{exception.class}: '#{exception.message}' - #{try} tries in #{elapsed_time} seconds and #{next_interval} seconds until the next try.")
      end
    }.freeze
  end
end
