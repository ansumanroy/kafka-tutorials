package com.kafkatutorials.streams.statestore;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.SessionWindows;

import java.time.Duration;

/**
 * SessionStore - stores values in session windows.
 * Merges sessions when activity continues.
 */
public class SessionStoreDemo {
    
    public static StreamsBuilder createTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        
        KStream<String, String> clicks = builder.stream("clicks");
        
        // Creates SessionStore automatically
        clicks.groupByKey()
            .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofMinutes(5)))
            .count(Materialized.as("session-counts"))
            .toStream()
            .to("session-output");
        
        return builder;
    }
}
