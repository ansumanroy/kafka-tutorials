package com.kafkatutorials.streams.aggregations;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;

import java.util.Properties;

/**
 * Count aggregation - simplest aggregation type.
 */
public class CountAggregation {
    
    public static StreamsBuilder createTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        
        KStream<String, String> events = builder.stream("events");
        
        // Count events per key
        events.groupByKey()
            .count(Materialized.as("event-counts"))
            .toStream()
            .to("event-count-results");
        
        return builder;
    }
    
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "count-aggregation");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, 
            System.getenv("KAFKA_BOOTSTRAP_SERVERS") != null ? System.getenv("KAFKA_BOOTSTRAP_SERVERS") : "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        
        KafkaStreams streams = new KafkaStreams(createTopology().build(), props);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
