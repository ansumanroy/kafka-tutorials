package com.kafkatutorials.streams.topology;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

/**
 * Demonstrates building complex topologies with sub-topologies and repartitioning.
 * 
 * Topology Design Best Practices:
 * 1. Minimize repartitioning (expensive)
 * 2. Filter early to reduce data volume
 * 3. Use sub-topologies for logical separation
 * 4. Name operations for debugging
 */
public class TopologyBuilder {
    
    /**
     * Complex topology with multiple branches and sub-topologies.
     */
    public static Topology buildComplexTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        
        // Source stream
        KStream<String, String> rawEvents = builder.stream("raw-events");
        
        // Sub-topology 1: Filtering and validation
        KStream<String, String> validEvents = rawEvents
            .filter((k, v) -> v != null, org.apache.kafka.streams.kstream.Named.as("null-filter"))
            .filter((k, v) -> !v.isEmpty(), org.apache.kafka.streams.kstream.Named.as("empty-filter"))
            .mapValues(String::trim, org.apache.kafka.streams.kstream.Named.as("trim"));
        
        // Sub-topology 2: Route by type
        validEvents
            .filter((k, v) -> v.startsWith("ERROR"), org.apache.kafka.streams.kstream.Named.as("error-filter"))
            .to("errors");
        
        validEvents
            .filter((k, v) -> v.startsWith("INFO"), org.apache.kafka.streams.kstream.Named.as("info-filter"))
            .to("info");
        
        // Sub-topology 3: Aggregation (causes repartitioning if key changed)
        validEvents
            .groupByKey()
            .count()
            .toStream()
            .to("event-counts");
        
        return builder.build();
    }
    
    /**
     * Print topology description for debugging.
     */
    public static void describeTopology(Topology topology) {
        System.out.println("=== Topology Description ===");
        System.out.println(topology.describe());
        System.out.println("\n=== Sub-topologies ===");
        topology.describe().subtopologies().forEach(sub -> {
            System.out.println("Sub-topology " + sub.id() + ":");
            sub.nodes().forEach(node -> 
                System.out.println("  - " + node.name())
            );
        });
    }
    
    public static void main(String[] args) {
        Topology topology = buildComplexTopology();
        describeTopology(topology);
        
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "topology-demo");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, 
            System.getenv("KAFKA_BOOTSTRAP_SERVERS") != null ? System.getenv("KAFKA_BOOTSTRAP_SERVERS") : "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        
        // Enable topology optimization
        props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
        
        KafkaStreams streams = new KafkaStreams(topology, props);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
