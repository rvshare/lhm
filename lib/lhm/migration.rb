# Copyright (c) 2011 - 2013, SoundCloud Ltd., Rany Keddo, Tobias Bielohlawek, Tobias
# Schmidt

require 'lhm/intersection'
require 'lhm/timestamp'

module Lhm
  class Migration
    attr_reader :origin, :destination, :conditions, :renames

    def initialize(origin, destination, conditions = nil, renames = {}, time = Time.now)
      @origin = origin
      @destination = destination
      @conditions = conditions
      @start = time
      @renames = renames
    end

    def archive_name
      "lhma_#{ startstamp }_#{ @origin.name }"[0...64]
    end

    def intersection
      Intersection.new(@origin, @destination, @renames)
    end

    def startstamp
      Timestamp.new(@start)
    end
  end
end
