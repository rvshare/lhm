require 'retriable'
require 'lhm/sql_helper'

module Lhm
  # RetryHelper standardizes the interface for retry behavior in components like
  # Entangler, AtomicSwitcher, ChunkerInsert.
  #
  # If an error includes the message "Lock wait timeout exceeded",
  # the RetryHelper will retry the SQL command again after about 500ms
  # for up to one hour.
  #
  # This behavior can be modified by calling `configure_retry` with options described in
  # https://github.com/kamui/retriable
  class SqlRetry
    def initialize(connection, options = {})
      @connection = connection
      @log_prefix = options.delete(:log_prefix)
      @retry_config = default_retry_config.dup.merge!(options)
    end

    def with_retries
      Retriable.retriable(retry_config) do
        yield(@connection)
      end
    end

    attr_reader :retry_config

    private

    # For a full list of configuration options see https://github.com/kamui/retriable
    def default_retry_config
      {
        on: {
          StandardError => [/Lock wait timeout exceeded/]
        },
        multiplier: 1, # each successive interval grows by this factor
        base_interval: 0.5, # the initial interval in seconds between tries.
        tries: 7200, # Number of attempts to make at running your code block (includes initial attempt).
        rand_factor: 0.25, # percentage to randomize the next retry interval time
        max_elapsed_time: Float::INFINITY, # max total time in seconds that code is allowed to keep being retried
        on_retry: Proc.new do |exception, try_number, total_elapsed_time, next_interval|
          log = "#{exception.class}: '#{exception.message}' - #{try_number} tries in #{total_elapsed_time} seconds and #{next_interval} seconds until the next try."
          log.prepend("[#{@log_prefix}] ") if @log_prefix
          Lhm.logger.info(log)
        end
      }.freeze
    end
  end
end
