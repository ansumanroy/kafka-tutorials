package com.kafkatutorials.streams.aggregations;

import com.google.gson.Gson;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;

/**
 * Custom aggregation with aggregate() - most flexible.
 * Example: Calculate running statistics (count, sum, avg).
 */
public class AggregateCustom {
    
    static class Stats {
        long count;
        double sum;
        double avg;
        
        public Stats() {}
        
        public Stats(long count, double sum) {
            this.count = count;
            this.sum = sum;
            this.avg = count > 0 ? sum / count : 0;
        }
    }
    
    public static StreamsBuilder createTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        Gson gson = new Gson();
        
        KStream<String, String> values = builder.stream("values");
        
        values
            .mapValues(v -> Double.parseDouble(v))
            .groupByKey()
            .aggregate(
                () -> new Stats(),  // Initializer
                (key, newValue, aggValue) -> {  // Adder
                    aggValue.count++;
                    aggValue.sum += newValue;
                    aggValue.avg = aggValue.sum / aggValue.count;
                    return aggValue;
                },
                Materialized.as("stats-store")
            )
            .toStream()
            .mapValues(value -> gson.toJson(value))
            .to("stats-results");
        
        return builder;
    }
}
