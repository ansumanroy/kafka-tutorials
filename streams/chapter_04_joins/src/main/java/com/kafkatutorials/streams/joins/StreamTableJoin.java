package com.kafkatutorials.streams.joins;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Demonstrates stream-table join (enrichment pattern).
 * 
 * Use Case: Enrich order events with user information
 * - Orders stream: high volume, real-time
 * - Users table: relatively static reference data
 * - Goal: Add user details to each order
 */
public class StreamTableJoin {
    private static final Logger logger = LoggerFactory.getLogger(StreamTableJoin.class);
    
    public static final String ORDERS_TOPIC = "orders";
    public static final String USERS_TOPIC = "users";
    public static final String ENRICHED_ORDERS_TOPIC = "orders-enriched";
    
    public static StreamsBuilder createTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        
        // Stream of orders: orderId -> "userId:amount:product"
        KStream<String, String> orders = builder.stream(ORDERS_TOPIC);
        
        // Table of users: userId -> "name:email:tier"
        KTable<String, String> users = builder.table(USERS_TOPIC);
        
        // Repartition orders by userId for join
        KStream<String, String> ordersByUserId = orders
            .selectKey((orderId, orderData) -> {
                // Extract userId from order data
                String[] parts = orderData.split(":");
                return parts.length > 0 ? parts[0] : null;
            });
        
        // Join stream with table (inner join)
        KStream<String, String> enrichedOrders = ordersByUserId.join(
            users,
            (orderData, userData) -> {
                String enriched = String.format("order=%s,user=%s", orderData, userData);
                logger.info("Enriched order: {}", enriched);
                return enriched;
            }
        );
        
        enrichedOrders.to(ENRICHED_ORDERS_TOPIC);
        
        // Left join variant: keep orders even if user not found
        KStream<String, String> allOrders = ordersByUserId.leftJoin(
            users,
            (orderData, userData) -> {
                if (userData != null) {
                    return String.format("order=%s,user=%s", orderData, userData);
                } else {
                    return String.format("order=%s,user=UNKNOWN", orderData);
                }
            }
        );
        
        allOrders.to("orders-all-enriched");
        
        return builder;
    }
    
    public static void main(String[] args) {
        String bootstrapServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS") != null 
            ? System.getenv("KAFKA_BOOTSTRAP_SERVERS") 
            : "localhost:9092";
        
        Properties props = JoinConfigHelper.getBasicConfig("stream-table-join", bootstrapServers);
        
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
            logger.info("Starting Stream-Table Join Demo...");
            streams.start();
            latch.await();
        } catch (Throwable e) {
            logger.error("Error", e);
            System.exit(1);
        }
        
        System.exit(0);
    }
}
