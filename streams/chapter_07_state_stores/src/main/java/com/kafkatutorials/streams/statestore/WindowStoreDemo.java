package com.kafkatutorials.streams.statestore;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;

import java.time.Duration;

/**
 * WindowStore - stores values in time-based windows.
 * Automatically created by windowed aggregations.
 */
public class WindowStoreDemo {
    
    public static StreamsBuilder createTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        
        KStream<String, String> events = builder.stream("events");
        
        // Creates WindowStore automatically
        events.groupByKey()
            .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5)))
            .count(Materialized.as("windowed-counts"))
            .toStream()
            .to("windowed-output");
        
        return builder;
    }
}
