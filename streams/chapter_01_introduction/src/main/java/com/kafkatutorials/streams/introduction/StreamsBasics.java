package com.kafkatutorials.streams.introduction;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Classic Word Count example - the "Hello World" of stream processing.
 * 
 * Topology:
 * 1. Read text lines from input topic
 * 2. Split lines into words
 * 3. Count occurrences of each word
 * 4. Write results to output topic
 * 
 * Example:
 * Input:  "hello world"
 * Input:  "hello kafka streams"
 * Output: hello=2, world=1, kafka=1, streams=1
 */
public class StreamsBasics {
    private static final Logger logger = LoggerFactory.getLogger(StreamsBasics.class);
    
    public static final String INPUT_TOPIC = "streams-plaintext-input";
    public static final String OUTPUT_TOPIC = "streams-wordcount-output";
    
    /**
     * Creates the word count topology.
     * 
     * @return StreamsBuilder with the word count topology
     */
    public static StreamsBuilder createWordCountTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        
        // 1. Read from input topic as a stream of strings
        KStream<String, String> textLines = builder.stream(INPUT_TOPIC);
        
        // 2. Process the stream
        KTable<String, Long> wordCounts = textLines
            // Split each line into words (flatMapValues)
            .flatMapValues(textLine -> Arrays.asList(textLine.toLowerCase().split("\\W+")))
            // Group by word (the value becomes the key)
            .groupBy((key, word) -> word)
            // Count occurrences
            .count();
        
        // 3. Write results to output topic
        wordCounts.toStream()
            .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));
        
        return builder;
    }
    
    /**
     * Main method to run the word count application.
     */
    public static void main(String[] args) {
        // Create configuration
        String bootstrapServers = StreamsConfigHelper.getBootstrapServers();
        Properties props = StreamsConfigHelper.getDevConfig("wordcount-application", bootstrapServers);
        
        // Build topology
        StreamsBuilder builder = createWordCountTopology();
        var topology = builder.build();
        
        // Print topology
        logger.info("Topology Description:\n{}", topology.describe());
        
        // Create and start streams application
        final KafkaStreams streams = new KafkaStreams(topology, props);
        
        // Add shutdown hook for graceful shutdown
        final CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                logger.info("Shutting down Kafka Streams application...");
                streams.close();
                latch.countDown();
            }
        });
        
        try {
            logger.info("Starting Word Count Streams application...");
            logger.info("Reading from topic: {}", INPUT_TOPIC);
            logger.info("Writing to topic: {}", OUTPUT_TOPIC);
            
            streams.start();
            latch.await();
        } catch (Throwable e) {
            logger.error("Error running streams application", e);
            System.exit(1);
        }
        
        System.exit(0);
    }
}
