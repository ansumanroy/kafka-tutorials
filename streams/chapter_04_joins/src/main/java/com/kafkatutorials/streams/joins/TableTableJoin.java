package com.kafkatutorials.streams.joins;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Demonstrates table-table join.
 * 
 * Use Case: Join user profile with user preferences
 * - Both are slowly changing tables
 * - Goal: Maintain current state of user+preferences
 */
public class TableTableJoin {
    private static final Logger logger = LoggerFactory.getLogger(TableTableJoin.class);
    
    public static final String PROFILES_TOPIC = "user-profiles";
    public static final String PREFERENCES_TOPIC = "user-preferences";
    public static final String COMPLETE_USERS_TOPIC = "users-complete";
    
    public static StreamsBuilder createTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        
        // Table of user profiles
        KTable<String, String> profiles = builder.table(PROFILES_TOPIC);
        
        // Table of user preferences
        KTable<String, String> preferences = builder.table(PREFERENCES_TOPIC);
        
        // Inner join: only users with both profile AND preferences
        KTable<String, String> completeUsers = profiles.join(
            preferences,
            (profileData, prefData) -> {
                String result = String.format("profile=%s,preferences=%s", profileData, prefData);
                logger.info("Complete user record: {}", result);
                return result;
            }
        );
        
        completeUsers.toStream().to(COMPLETE_USERS_TOPIC);
        
        // Left join: all profiles, with preferences if available
        KTable<String, String> allProfiles = profiles.leftJoin(
            preferences,
            (profileData, prefData) -> {
                if (prefData != null) {
                    return String.format("profile=%s,preferences=%s", profileData, prefData);
                } else {
                    return String.format("profile=%s,preferences=DEFAULTS", profileData);
                }
            }
        );
        
        allProfiles.toStream().to("users-all-complete");
        
        return builder;
    }
    
    public static void main(String[] args) {
        String bootstrapServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS") != null 
            ? System.getenv("KAFKA_BOOTSTRAP_SERVERS") 
            : "localhost:9092";
        
        Properties props = JoinConfigHelper.getBasicConfig("table-table-join", bootstrapServers);
        
        StreamsBuilder builder = createTopology();
        var topology = builder.build();
        
        logger.info("Topology:\n{}", topology.describe());
        
        final KafkaStreams streams = new KafkaStreams(topology, props);
        
        final CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down...");
            streams.close();
            latch.countDown();
        }));
        
        try {
            logger.info("Starting Table-Table Join Demo...");
            streams.start();
            latch.await();
        } catch (Throwable e) {
            logger.error("Error", e);
            System.exit(1);
        }
        
        System.exit(0);
    }
}
