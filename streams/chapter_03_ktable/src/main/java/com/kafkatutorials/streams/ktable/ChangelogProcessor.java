package com.kafkatutorials.streams.ktable;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Demonstrates changelog processing with KTable.
 * Shows how to track and react to changes (updates, deletes).
 */
public class ChangelogProcessor {
    private static final Logger logger = LoggerFactory.getLogger(ChangelogProcessor.class);
    
    public static final String CHANGELOG_TOPIC = "user-changelog";
    public static final String CHANGES_TOPIC = "user-changes";
    
    public static StreamsBuilder createTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        
        // Read changelog as KTable
        KTable<String, String> userTable = builder.table(CHANGELOG_TOPIC);
        
        // Process changes
        // Note: KTable transformations also see deletions (null values)
        userTable
            .toStream()
            .peek((key, newValue) -> {
                if (newValue == null) {
                    logger.info("DELETE: User {} was deleted", key);
                } else {
                    logger.info("UPDATE: User {} -> {}", key, newValue);
                }
            })
            .filter((key, value) -> value != null) // Keep only updates, filter deletes
            .mapValues((key, value) -> 
                String.format("Change detected for %s: %s", key, value)
            )
            .to(CHANGES_TOPIC);
        
        return builder;
    }
    
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "changelog-processor");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, 
            System.getenv("KAFKA_BOOTSTRAP_SERVERS") != null 
                ? System.getenv("KAFKA_BOOTSTRAP_SERVERS") 
                : "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0L);
        
        StreamsBuilder builder = createTopology();
        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        
        final CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close();
            latch.countDown();
        }));
        
        try {
            logger.info("Starting Changelog Processor...");
            streams.start();
            latch.await();
        } catch (Throwable e) {
            logger.error("Error", e);
            System.exit(1);
        }
        
        System.exit(0);
    }
}
