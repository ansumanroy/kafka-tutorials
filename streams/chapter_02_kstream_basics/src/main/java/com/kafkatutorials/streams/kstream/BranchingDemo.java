package com.kafkatutorials.streams.kstream;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Demonstrates branching: splitting a stream into multiple streams based on predicates.
 * 
 * Use Case: Route messages to different topics based on content/type.
 * 
 * Example: Route log messages by severity (ERROR, WARN, INFO, DEBUG)
 */
public class BranchingDemo {
    private static final Logger logger = LoggerFactory.getLogger(BranchingDemo.class);
    
    public static final String INPUT_TOPIC = "kstream-branch-input";
    public static final String ERROR_TOPIC = "kstream-branch-error";
    public static final String WARN_TOPIC = "kstream-branch-warn";
    public static final String INFO_TOPIC = "kstream-branch-info";
    public static final String DEBUG_TOPIC = "kstream-branch-debug";
    
    public static StreamsBuilder createTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        
        KStream<String, String> input = builder.stream(INPUT_TOPIC);
        
        // Branch based on log level prefix
        Map<String, KStream<String, String>> branches = input.split()
            .branch((key, value) -> value != null && value.startsWith("ERROR:"), 
                    Branched.as("error"))
            .branch((key, value) -> value != null && value.startsWith("WARN:"), 
                    Branched.as("warn"))
            .branch((key, value) -> value != null && value.startsWith("INFO:"), 
                    Branched.as("info"))
            .branch((key, value) -> value != null && value.startsWith("DEBUG:"), 
                    Branched.as("debug"))
            .noDefaultBranch();
        
        // Route each branch to appropriate topic
        if (branches.containsKey("error")) {
            branches.get("error")
                .peek((k, v) -> logger.info("ERROR: {}", v))
                .to(ERROR_TOPIC);
        }
        
        if (branches.containsKey("warn")) {
            branches.get("warn")
                .peek((k, v) -> logger.info("WARN: {}", v))
                .to(WARN_TOPIC);
        }
        
        if (branches.containsKey("info")) {
            branches.get("info")
                .peek((k, v) -> logger.info("INFO: {}", v))
                .to(INFO_TOPIC);
        }
        
        if (branches.containsKey("debug")) {
            branches.get("debug")
                .peek((k, v) -> logger.info("DEBUG: {}", v))
                .to(DEBUG_TOPIC);
        }
        
        return builder;
    }
    
    public static void main(String[] args) {
        String bootstrapServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS") != null 
            ? System.getenv("KAFKA_BOOTSTRAP_SERVERS") 
            : "localhost:9092";
        
        Properties props = TransformationHelper.getBasicConfig(
            "branching-demo", 
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
            logger.info("Starting BranchingDemo...");
            logger.info("Reading from: {}", INPUT_TOPIC);
            logger.info("Writing to: {}, {}, {}, {}", ERROR_TOPIC, WARN_TOPIC, INFO_TOPIC, DEBUG_TOPIC);
            streams.start();
            latch.await();
        } catch (Throwable e) {
            logger.error("Error", e);
            System.exit(1);
        }
        
        System.exit(0);
    }
}
