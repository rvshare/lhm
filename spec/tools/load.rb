require "active_record"
require "optparse"
require "yaml"

settings = {
  :number   => 50_000,
  :wait     => 0,
  :slowdown => 0
}

options = OptionParser.new do |o|
  o.on("-n", "--number x",   Integer,  "number of queries [default: #{settings[:number]}]")                       { |o| settings[:number] = o }
  o.on("-w", "--wait x",     Integer,  "time to wait between transactions [default: #{settings[:wait]}]")         { |o| settings[:wait] = o }
  o.on("-s", "--slowdown x", Integer,  "slowdown query execution by x seconds [default: #{settings[:slowdown]}]") { |o| settings[:slowdown] = o }
  o.on("-h", "--help",                    "Print the usage")              {|o| puts options.to_s; exit }
end
options.parse(ARGV)

config = YAML.load_file(File.expand_path("../../config/database.yml", __FILE__))
ActiveRecord::Base.establish_connection(config)

def connection
  ActiveRecord::Base.connection
end

settings[:number].times do |index|
  case rand
  when 0...0.34
    time = Time.now.utc
    connection.insert("INSERT INTO test (user, created_at, updated_at) VALUES ('hans #{rand}', '#{time}', '#{time}')")
  when 0.34...0.67
    connection.update("UPDATE test SET user = 'u|' + user, updated_at = '#{Time.now.utc}' WHERE id = #{index - (rand * 100).to_i}")
  when 0.67..1
    connection.update("DELETE FROM test WHERE id = #{index - (rand * 100).to_i}")
  end
end