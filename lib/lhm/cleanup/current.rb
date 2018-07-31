module Lhm
  module Cleanup
    class Current
      def initialize(run, origin_table_name, connection)
        @run = run
        @origin_table_name = origin_table_name
        @connection = connection
      end

      attr_reader :run, :origin_table_name, :connection

      def execute
        if run
          drop_lhm_triggers_for_origin
          drop_lhmn_tables_for_origin
        else
          report
        end
      end

      private

      def drop_lhm_triggers_for_origin
        lhm_triggers_for_origin.each do |trigger|
          connection.execute("drop trigger if exists #{trigger}")
        end
      end

      def lhm_triggers_for_origin
        @lhm_triggers_for_origin ||= all_triggers_for_origin.select { |name| name =~ /^lhmt/ }
      end

      def all_triggers_for_origin
        @all_triggers_for_origin ||= connection.select_values("show triggers like '%#{origin_table_name}'").collect do |trigger|
          trigger.respond_to?(:trigger) ? trigger.trigger : trigger
        end
      end

      def drop_lhmn_tables_for_origin
        lhmn_tables_for_origin.each do |table|
          connection.execute("drop table if exists #{table}")
        end
      end

      def lhmn_tables_for_origin
        @lhmn_tables_for_origin ||= connection.select_values("show tables like 'lhmn_#{origin_table_name}'")
      end

      def report
        if lhmn_tables_for_origin.empty? && lhm_triggers_for_origin.empty?
          puts 'Everything is clean. Nothing to do.'
          true
        else
          puts "Would drop LHM backup tables: #{lhmn_tables_for_origin.join(', ')}."
          puts "Would drop LHM triggers: #{lhm_triggers_for_origin.join(', ')}."
          puts 'Run with Lhm.cleanup(true) to drop all LHM triggers and tables, or Lhm.cleanup_current_run(true, table_name) to clean up a specific LHM.'
          false
        end
      end
    end
  end
end
