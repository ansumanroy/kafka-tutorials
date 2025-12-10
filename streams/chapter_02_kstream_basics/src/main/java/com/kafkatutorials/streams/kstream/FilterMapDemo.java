package com.kafkatutorials.streams.kstream;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Demonstrates stateless KStream operations: filter, map, flatMap, foreach.
 * 
 * Topology:
 * 1. Read messages from input topic
 * 2. Filter: Keep only messages longer than 5 characters
 * 3. Map: Transform to uppercase
 * 4. FlatMap: Split into words
 * 5. Foreach: Log each word
 * 6. Write to output topic
 */
public class FilterMapDemo {
    private static final Logger logger = LoggerFactory.getLogger(FilterMapDemo.class);
    
    public static final String INPUT_TOPIC = "kstream-filter-input";
    public static final String OUTPUT_TOPIC = "kstream-filter-output";
    
    public static StreamsBuilder createTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        
        // Source stream
        KStream<String, String> input = builder.stream(INPUT_TOPIC);
        
        // 1. Filter: Keep messages longer than 5 characters
        KStream<String, String> filtered = input
            .filter((key, value) -> {
                boolean keep = value != null && value.length() > 5;
                logger.debug("Filter: key={}, value={}, keep={}", key, value, keep);
                return keep;
            });
        
        // 2. Map: Transform to uppercase
        KStream<String, String> mapped = filtered
            .mapValues(value -> {
                String result = value.toUpperCase();
                logger.debug("Map: {} -> {}", value, result);
                return result;
            });
        
        // 3. FlatMap: Split into individual words
        KStream<String, String> flatMapped = mapped
            .flatMapValues(value -> {
                var words = TransformationHelper.splitIntoWords(value);
                logger.debug("FlatMap: {} -> {}", value, words);
                return words;
            });
        
        // 4. Foreach: Side effect (logging)
        flatMapped.foreach((key, word) -> 
            logger.info("Processing word: key={}, word={}", key, word)
        );
        
        // 5. Write to output
        flatMapped.to(OUTPUT_TOPIC);
        
        return builder;
    }
    
    public static void main(String[] args) {
        String bootstrapServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS") != null 
            ? System.getenv("KAFKA_BOOTSTRAP_SERVERS") 
            : "localhost:9092";
        
        Properties props = TransformationHelper.getBasicConfig(
            "filter-map-demo", 
            bootstrapServers
        );
        
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
            logger.info("Starting FilterMapDemo...");
            logger.info("Reading from: {}", INPUT_TOPIC);
            logger.info("Writing to: {}", OUTPUT_TOPIC);
            streams.start();
            latch.await();
        } catch (Throwable e) {
            logger.error("Error", e);
            System.exit(1);
        }
        
        System.exit(0);
    }
}
