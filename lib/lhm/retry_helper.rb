require 'retriable'
require 'lhm/sql_helper'

module Lhm
  # RetryHelper standardizes the interface for retry behavior in components like
  # Entangler and AtomicSwitcher.
  #
  # It expects the caller to implement:
  # DEFAULT_MAX_RETRIES
  # DEFAULT_RETRY_WAIT
  #
  # These defaults can be overridden by setting an instance variable with corresponding names:
  # @max_retries
  # @retry_wait
  #
  # To retry some behavior, use `execute_with_retries(statement)`
  # which assumes `@connection` is available.
  module RetryHelper
    def self.included(base)
      raise(ArgumentError, "#{base} must define DEFAULT_MAX_RETRIES before calling 'include RetryHelper'") unless base.constants.include?(:DEFAULT_MAX_RETRIES)
      raise(ArgumentError, "#{base} must define DEFAULT_RETRY_WAIT before calling 'include RetryHelper'") unless base.constants.include?(:DEFAULT_RETRY_WAIT)
    end

    def execute_with_retries(statement)
      Retriable.retriable(retry_config) do
        @connection.execute(SqlHelper.tagged(statement))
      end
    end

    private

    def retry_config
      {
        on: {
          StandardError => [/Lock wait timeout exceeded/]
        },
        tries: @max_retries || self.class::DEFAULT_MAX_RETRIES, # number of attempts
        base_interval: @retry_wait || self.class::DEFAULT_RETRY_WAIT, # initial interval in seconds between tries
        multiplier: 1.5, # each successive interval grows by this factor
        rand_factor: 0.25, # percentage to randomize the next retry interval time
        max_elapsed_time: Float::INFINITY, # max total time in seconds that code is allowed to keep being retried
        on_retry: Proc.new do |exception, try, elapsed_time, next_interval|
          Lhm.logger.info("#{exception.class}: '#{exception.message}' - #{try} tries in #{elapsed_time} seconds and #{next_interval} seconds until the next try.")
        end
      }
    end
  end
end
