package com.kafkatutorials.streams.ktable;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Demonstrates KTable - a changelog stream where each record represents 
 * an update to a key's value.
 * 
 * Use Case: User profile updates
 * - Key: user_id
 * - Value: user profile (latest state)
 * 
 * KTable maintains the latest value for each key.
 */
public class KTableDemo {
    private static final Logger logger = LoggerFactory.getLogger(KTableDemo.class);
    
    public static final String USER_UPDATES_TOPIC = "user-updates";
    public static final String USER_PROFILE_TOPIC = "user-profiles";
    
    public static StreamsBuilder createTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        
        // 1. Read user updates as KTable (changelog)
        // Each record is an update to the user's profile
        KTable<String, String> userTable = builder.table(
            USER_UPDATES_TOPIC,
            Materialized.as("user-profiles-store")
        );
        
        // 2. Filter: Keep only active users
        KTable<String, String> activeUsers = userTable
            .filter((userId, profile) -> {
                if (profile != null && profile.contains("status:active")) {
                    logger.info("Active user: {} -> {}", userId, profile);
                    return true;
                }
                logger.info("Inactive user (filtered): {} -> {}", userId, profile);
                return false;
            });
        
        // 3. Transform: Add timestamp
        KTable<String, String> enriched = activeUsers
            .mapValues((readOnlyKey, value) -> {
                String enrichedValue = value + ",updated:" + System.currentTimeMillis();
                logger.info("Enriched: {} -> {}", readOnlyKey, enrichedValue);
                return enrichedValue;
            });
        
        // 4. Convert to stream and write to output topic
        // Note: toStream() converts KTable changelog → KStream of updates
        enriched.toStream().to(USER_PROFILE_TOPIC);
        
        // 5. Also log all changes
        userTable.toStream()
            .foreach((userId, profile) -> 
                logger.info("User update: userId={}, profile={}", userId, profile)
            );
        
        return builder;
    }
    
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "ktable-demo");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, 
            System.getenv("KAFKA_BOOTSTRAP_SERVERS") != null 
                ? System.getenv("KAFKA_BOOTSTRAP_SERVERS") 
                : "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0L); // Disable caching for demo
        
        StreamsBuilder builder = createTopology();
        var topology = builder.build();
        
        logger.info("Topology:\n{}", topology.describe());
        
        final KafkaStreams streams = new KafkaStreams(topology, props);
        
        final CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown") {
            @Override
            public void run() {
                logger.info("Shutting down...");
                streams.close();
                latch.countDown();
            }
        });
        
        try {
            logger.info("Starting KTable Demo...");
            logger.info("Reading from: {}", USER_UPDATES_TOPIC);
            logger.info("Writing to: {}", USER_PROFILE_TOPIC);
            streams.start();
            latch.await();
        } catch (Throwable e) {
            logger.error("Error", e);
            System.exit(1);
        }
        
        System.exit(0);
    }
}
