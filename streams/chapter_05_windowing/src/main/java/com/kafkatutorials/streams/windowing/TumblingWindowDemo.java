package com.kafkatutorials.streams.windowing;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;

/**
 * Tumbling windows: Fixed-size, non-overlapping time windows.
 * Example: Count page views per 1-minute window.
 */
public class TumblingWindowDemo {
    private static final Logger logger = LoggerFactory.getLogger(TumblingWindowDemo.class);
    
    public static StreamsBuilder createTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        
        KStream<String, String> pageViews = builder.stream("page-views");
        
        // Count page views per page per 1-minute tumbling window
        TimeWindowedKStream<String, String> windowed = pageViews
            .groupByKey()
            .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)));
        
        windowed.count()
            .toStream()
            .map((windowedKey, count) -> {
                String key = windowedKey.key();
                long windowStart = windowedKey.window().start();
                String result = String.format("%s,window=%d,count=%d", key, windowStart, count);
                logger.info(result);
                return KeyValue.pair(key, result);
            })
            .to("page-views-counts");
        
        return builder;
    }
    
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "tumbling-window-demo");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, 
            System.getenv("KAFKA_BOOTSTRAP_SERVERS") != null ? System.getenv("KAFKA_BOOTSTRAP_SERVERS") : "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        
        KafkaStreams streams = new KafkaStreams(createTopology().build(), props);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
