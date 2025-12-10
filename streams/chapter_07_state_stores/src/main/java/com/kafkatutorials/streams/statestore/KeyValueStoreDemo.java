package com.kafkatutorials.streams.statestore;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.Properties;

/**
 * KeyValueStore - most common state store type.
 * Backed by RocksDB (persistent) with changelog for fault tolerance.
 */
public class KeyValueStoreDemo {
    
    public static StreamsBuilder createTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        
        // Create key-value store via aggregation
        KStream<String, String> events = builder.stream("events");
        
        events.groupByKey()
            .count(Materialized.as("event-counts"))  // Creates KeyValueStore
            .toStream()
            .to("counts-output");
        
        return builder;
    }
    
    /**
     * Create custom key-value store explicitly.
     */
    public static StoreBuilder<KeyValueStore<String, Long>> createCustomStore() {
        return Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore("custom-store"),
            Serdes.String(),
            Serdes.Long()
        );
    }
    
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kv-store-demo");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, 
            System.getenv("KAFKA_BOOTSTRAP_SERVERS") != null ? System.getenv("KAFKA_BOOTSTRAP_SERVERS") : "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        
        KafkaStreams streams = new KafkaStreams(createTopology().build(), props);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
