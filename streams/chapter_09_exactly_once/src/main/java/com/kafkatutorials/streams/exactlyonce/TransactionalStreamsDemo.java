package com.kafkatutorials.streams.exactlyonce;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

/**
 * Demonstrates transactional processing in Kafka Streams.
 * 
 * With exactly-once semantics:
 * - Read from input topic
 * - Process (stateful operations)
 * - Write to output topic
 * - Commit offsets
 * 
 * All happen atomically within a transaction.
 */
public class TransactionalStreamsDemo {
    
    public static StreamsBuilder createTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        
        KStream<String, String> orders = builder.stream("orders");
        
        // Multi-step processing - all atomic with EOS
        orders
            // Step 1: Validate
            .filter((k, v) -> v != null && !v.isEmpty())
            
            // Step 2: Transform
            .mapValues(v -> v.toUpperCase())
            
            // Step 3: Aggregate
            .groupByKey()
            .count()
            
            // Step 4: Write results
            .toStream()
            .to("order-counts");
        
        // With EOS: Either ALL steps complete or NONE
        // No partial results, no duplicates
        
        return builder;
    }
}
