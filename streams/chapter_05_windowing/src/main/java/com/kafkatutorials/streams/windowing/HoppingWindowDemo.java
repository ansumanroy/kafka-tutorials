package com.kafkatutorials.streams.windowing;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.TimeWindows;

import java.time.Duration;

/**
 * Hopping windows: Fixed-size, overlapping windows.
 * Example: 5-minute windows, advancing every 1 minute.
 */
public class HoppingWindowDemo {
    
    public static StreamsBuilder createTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        
        KStream<String, String> events = builder.stream("events");
        
        // 5-minute windows, advancing every 1 minute (overlap)
        events
            .groupByKey()
            .windowedBy(TimeWindows.ofSizeAndGrace(
                Duration.ofMinutes(5),    // Window size
                Duration.ofSeconds(30)    // Grace period
            ).advanceBy(Duration.ofMinutes(1)))  // Advance/hop
            .count()
            .toStream()
            .to("events-hopping-counts");
        
        return builder;
    }
}
