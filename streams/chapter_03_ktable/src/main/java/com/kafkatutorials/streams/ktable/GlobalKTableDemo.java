package com.kafkatutorials.streams.ktable;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Demonstrates GlobalKTable - a fully replicated table available to all instances.
 * 
 * Difference from KTable:
 * - KTable: Partitioned (each instance has subset of data)
 * - GlobalKTable: Fully replicated (each instance has ALL data)
 * 
 * Use Case: Enrich transaction stream with product catalog
 * - Transactions stream: high volume, partitioned
 * - Product catalog: small, needs to be available for all transactions
 */
public class GlobalKTableDemo {
    private static final Logger logger = LoggerFactory.getLogger(GlobalKTableDemo.class);
    
    public static final String TRANSACTIONS_TOPIC = "transactions";
    public static final String PRODUCTS_TOPIC = "products";
    public static final String ENRICHED_TRANSACTIONS_TOPIC = "enriched-transactions";
    
    public static StreamsBuilder createTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        
        // 1. Read product catalog as GlobalKTable
        // Each instance gets a full copy of all products
        GlobalKTable<String, String> productTable = builder.globalTable(PRODUCTS_TOPIC);
        
        // 2. Read transactions as stream
        KStream<String, String> transactions = builder.stream(TRANSACTIONS_TOPIC);
        
        // 3. Enrich transactions with product information
        // Join stream with GlobalKTable (no co-partitioning required!)
        KStream<String, String> enriched = transactions.join(
            productTable,
            // Key mapper: extract product_id from transaction
            (txnKey, txnValue) -> {
                // Format: "transaction_id:product_id:quantity"
                String[] parts = txnValue.split(":");
                return parts.length > 1 ? parts[1] : null; // product_id
            },
            // Value joiner: combine transaction with product info
            (txnValue, productInfo) -> {
                logger.info("Enriching transaction {} with product {}", txnValue, productInfo);
                return String.format("%s,product_info:%s", txnValue, productInfo);
            }
        );
        
        // 4. Write enriched transactions
        enriched.to(ENRICHED_TRANSACTIONS_TOPIC);
        
        return builder;
    }
    
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "global-ktable-demo");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, 
            System.getenv("KAFKA_BOOTSTRAP_SERVERS") != null 
                ? System.getenv("KAFKA_BOOTSTRAP_SERVERS") 
                : "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        
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
            logger.info("Starting GlobalKTable Demo...");
            logger.info("Reading transactions from: {}", TRANSACTIONS_TOPIC);
            logger.info("Reading products from: {}", PRODUCTS_TOPIC);
            logger.info("Writing to: {}", ENRICHED_TRANSACTIONS_TOPIC);
            streams.start();
            latch.await();
        } catch (Throwable e) {
            logger.error("Error", e);
            System.exit(1);
        }
        
        System.exit(0);
    }
}
