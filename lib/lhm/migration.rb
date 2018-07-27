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

    def origin_name
      @origin_name ||= origin.name
    end

    def origin_columns
      @origin_columns ||= intersection.origin.typed(origin_name)
    end

    def destination_name
      @destination_name ||= destination.name
    end

    def destination_columns
      @destination_columns ||= intersection.destination.joined
    end
  end
end
