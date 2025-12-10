package com.kafkatutorials.streams.windowing;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.SessionWindows;

import java.time.Duration;

/**
 * Session windows: Activity-based, variable-size windows.
 * Example: Group user activity into sessions (5 min inactivity gap).
 */
public class SessionWindowDemo {
    
    public static StreamsBuilder createTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        
        KStream<String, String> clicks = builder.stream("user-clicks");
        
        // Group clicks into sessions (5 min inactivity closes session)
        clicks
            .groupByKey()
            .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofMinutes(5)))
            .count()
            .toStream()
            .to("user-sessions");
        
        return builder;
    }
}
