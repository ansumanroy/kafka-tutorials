package com.kafkatutorials.streams.joins;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Demonstrates stream-stream joins.
 * 
 * Use Case: Join click events with impression events
 * - Impressions: Ad shown to user
 * - Clicks: User clicked on ad
 * - Goal: Measure click-through rate
 * 
 * Windowing required because streams are unbounded.
 */
public class StreamStreamJoin {
    private static final Logger logger = LoggerFactory.getLogger(StreamStreamJoin.class);
    
    public static final String IMPRESSIONS_TOPIC = "ad-impressions";
    public static final String CLICKS_TOPIC = "ad-clicks";
    public static final String JOINED_TOPIC = "ad-click-through";
    
    public static StreamsBuilder createTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        
        KStream<String, String> impressions = builder.stream(IMPRESSIONS_TOPIC);
        KStream<String, String> clicks = builder.stream(CLICKS_TOPIC);
        
        // Join window: clicks must occur within 5 minutes of impression
        JoinWindows joinWindow = JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(5));
        
        // Inner join: only when both impression AND click exist
        KStream<String, String> clickThrough = impressions.join(
            clicks,
            (impressionValue, clickValue) -> {
                String result = String.format(
                    "impression=%s,click=%s,matched=true",
                    impressionValue, clickValue
                );
                logger.info("Matched click-through: {}", result);
                return result;
            },
            joinWindow
        );
        
        clickThrough.to(JOINED_TOPIC);
        
        // Left join: all impressions, with clicks if available
        KStream<String, String> allImpressions = impressions.leftJoin(
            clicks,
            (impressionValue, clickValue) -> {
                if (clickValue != null) {
                    return String.format("impression=%s,clicked=yes", impressionValue);
                } else {
                    return String.format("impression=%s,clicked=no", impressionValue);
                }
            },
            joinWindow
        );
        
        allImpressions
            .peek((k, v) -> logger.info("Impression result: key={}, value={}", k, v))
            .to("ad-impressions-enriched");
        
        return builder;
    }
    
    public static void main(String[] args) {
        String bootstrapServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS") != null 
            ? System.getenv("KAFKA_BOOTSTRAP_SERVERS") 
            : "localhost:9092";
        
        Properties props = JoinConfigHelper.getBasicConfig("stream-stream-join", bootstrapServers);
        
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
            logger.info("Starting Stream-Stream Join Demo...");
            streams.start();
            latch.await();
        } catch (Throwable e) {
            logger.error("Error", e);
            System.exit(1);
        }
        
        System.exit(0);
    }
}
