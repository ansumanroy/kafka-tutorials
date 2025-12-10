package com.kafkatutorials.streams.exactlyonce;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Demonstrates exactly-once semantics in Kafka Streams.
 * 
 * EOS guarantees:
 * - Messages processed exactly once (no duplicates)
 * - State updates atomic with output
 * - Survives failures without data loss or duplication
 */
public class EOSProducerDemo {
    private static final Logger logger = LoggerFactory.getLogger(EOSProducerDemo.class);
    
    public static StreamsBuilder createTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        
        KStream<String, String> input = builder.stream("eos-input");
        
        // Simple transformation with count
        input.groupByKey()
            .count()
            .toStream()
            .peek((k, v) -> logger.info("Processed: key={}, count={}", k, v))
            .to("eos-output");
        
        return builder;
    }
    
    public static void main(String[] args) {
        String bootstrapServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS") != null 
            ? System.getenv("KAFKA_BOOTSTRAP_SERVERS") 
            : "localhost:9092";
        
        // Use exactly-once configuration
        Properties props = EOSConfigHelper.getEOSConfig("eos-demo", bootstrapServers);
        
        // For comparison, could use at-least-once:
        // Properties props = EOSConfigHelper.getAtLeastOnceConfig("eos-demo", bootstrapServers);
        
        logger.info("Processing guarantee: {}", 
            props.getProperty(StreamsConfig.PROCESSING_GUARANTEE_CONFIG));
        
        KafkaStreams streams = new KafkaStreams(createTopology().build(), props);
        
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down...");
            streams.close();
        }));
        
        logger.info("Starting EOS Demo with exactly-once semantics...");
        streams.start();
    }
}
