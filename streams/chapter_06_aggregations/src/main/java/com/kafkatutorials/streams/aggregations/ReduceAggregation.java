package com.kafkatutorials.streams.aggregations;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;

/**
 * Reduce aggregation - combines values for same key.
 * Example: Sum, max, concatenation.
 */
public class ReduceAggregation {
    
    public static StreamsBuilder createTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        
        KStream<String, Integer> numbers = builder.stream("numbers");
        
        // Sum all numbers for each key
        numbers.groupByKey()
            .reduce(
                Integer::sum,
                Materialized.as("number-sums")
            )
            .toStream()
            .to("number-sum-results");
        
        return builder;
    }
}
