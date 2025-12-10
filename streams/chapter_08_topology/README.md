# Chapter 08 - Topology Design and Testing

Best practices for designing and testing Kafka Streams topologies.

## Topology Concepts

**Topology** = DAG (Directed Acyclic Graph) of processors and state stores.

**Sub-topologies** = Independently scalable parts of topology. Created when:
- Operations require repartitioning
- Operations require different parallelism

## Design Best Practices

### 1. Minimize Repartitioning

Repartitioning is expensive (network I/O, disk I/O).

```java
// ❌ Bad: Unnecessary repartitioning
stream
    .selectKey((k, v) -> newKey)  // Repartition
    .groupByKey()
    .count();

// ✅ Good: Use groupBy directly
stream
    .groupBy((k, v) -> newKey)  // One repartition
    .count();
```

### 2. Filter Early

Reduce data volume as early as possible.

```java
// ✅ Good: Filter first
stream
    .filter(predicate)      // Reduce volume
    .mapValues(expensive)   // Process less data
```

### 3. Name Operations

Helps with debugging and monitoring.

```java
stream
    .filter(predicate, Named.as("valid-events"))
    .mapValues(transform, Named.as("enrich"));
```

### 4. Topology Optimization

Enable automatic optimization.

```java
props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
```

## Testing Strategies

### Unit Tests (TopologyTestDriver)

Fast, deterministic, no Kafka cluster needed.

```java
TopologyTestDriver testDriver = new TopologyTestDriver(topology, props);

TestInputTopic<String, String> input = testDriver.createInputTopic(...);
TestOutputTopic<String, String> output = testDriver.createOutputTopic(...);

input.pipeInput("key", "value");
assertEquals("expected", output.readValue());
```

### Integration Tests

Test against real Kafka cluster.

```java
@Test
void testWithKafka() {
    // Start streams app
    KafkaStreams streams = new KafkaStreams(topology, props);
    streams.start();
    
    // Produce to input topic
    // Consume from output topic
    // Assert results
    
    streams.close();
}
```

## Topology Visualization

```java
System.out.println(topology.describe());
```

Output shows:
- Sub-topologies
- Processor nodes
- Connections
- State stores

## Key Takeaways

1. **Sub-topologies** = independently scalable units
2. **Minimize repartitioning** for performance
3. **Filter early** to reduce data volume
4. **Name operations** for observability
5. **TopologyTestDriver** for unit tests
6. **Enable optimization** in production

**Next**: Chapter 09 - Exactly-Once Semantics
