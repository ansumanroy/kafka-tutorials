---
marp: true
theme: default
paginate: true
headingDivider: 2
---

<!-- 
To export: 
marp STREAMS_SLIDES.md --pdf --allow-local-files --html
marp STREAMS_SLIDES.md --pptx --allow-local-files --html
marp STREAMS_SLIDES.md --html --allow-local-files
-->

<style>
section {
  font-size: 24px;
}
code {
  font-size: 18px;
}
</style>

# Kafka Streams Tutorial

## From Basics to Production

*Comprehensive guide to Apache Kafka Streams*

---

## What We'll Cover

1. **Basics** (Ch 01-03): Introduction, KStream, KTable
2. **Advanced** (Ch 04-07): Joins, Windows, Aggregations, State
3. **Production** (Ch 08-09): Topology Design, Exactly-Once
4. **Integration** (Ch 10): KSQL/ksqlDB

---

# Chapter 01
## Introduction to Kafka Streams

---

## What is Kafka Streams?

**Kafka Streams** is a client library for building stream processing applications.

**Key Features:**
- ✅ Standard Java application (no cluster needed)
- ✅ Exactly-once semantics
- ✅ Stateful and stateless processing
- ✅ Fault-tolerant
- ✅ Scalable

---

## KStream vs KTable

| Aspect | KStream | KTable |
|--------|---------|--------|
| **Semantic** | Record stream | Changelog stream |
| **Records** | Independent events | Updates to state |
| **Example** | Clicks, orders | User profiles, inventory |

---

## Word Count Example

```java
KStream<String, String> textLines = builder.stream("input");

KTable<String, Long> wordCounts = textLines
    .flatMapValues(line -> Arrays.asList(line.split(" ")))
    .groupBy((key, word) -> word)
    .count();

wordCounts.toStream().to("output");
```

**Input:** `"hello world hello"`
**Output:** `hello=2, world=1`

---

# Chapter 02
## KStream Basics - Stateless Operations

---

## Common KStream Operations

| Operation | Purpose | Example |
|-----------|---------|---------|
| `filter()` | Keep matching records | Remove nulls |
| `map()` | Transform key+value | Change both |
| `mapValues()` | Transform value only | Uppercase |
| `flatMapValues()` | One → many | Split sentences |
| `branch()` | Route by condition | Split by type |

---

## Filter and Map

```java
stream
    .filter((k, v) -> v != null && v.length() > 5)
    .mapValues(String::toUpperCase)
    .to("output");
```

**Key Point:** Use `mapValues()` when possible to avoid repartitioning!

---

## FlatMapValues

Transform one record into multiple:

```java
stream
    .flatMapValues(line -> Arrays.asList(line.split(" ")))
    .to("words");
```

**Input:** `"hello world"`
**Output:** `"hello"`, `"world"` (2 records)

---

# Chapter 03
## KTable - Stateful Processing

---

## KTable Semantics

**KTable** = Latest state for each key

```
Time: ─────────────────────────>
      user1="Alice"  user1="Alice Smith"  user1="Alice S."
      
Current State: user1="Alice S." (only latest)
```

**Null value = deletion (tombstone)**

---

## GlobalKTable

**Fully replicated** to all instances (not partitioned)

**Use for:**
- Small reference data (products, config)
- Lookup tables
- No co-partitioning needed

```java
GlobalKTable<String, String> products = builder.globalTable("products");
```

---

## Changelog Topics

Kafka Streams automatically creates changelog topics:

- **Format:** `{app-id}-{store-name}-changelog`
- **Purpose:** Fault tolerance
- **Always compacted:** `cleanup.policy=compact`

---

# Chapter 04
## Joins

---

## Join Types Matrix

| Left | Right | Join Types | Windowing | Use Case |
|------|-------|------------|-----------|----------|
| **KStream** | **KStream** | Inner, Left, Outer | **Required** | Correlate events |
| **KStream** | **KTable** | Inner, Left | Not required | Enrich stream |
| **KTable** | **KTable** | Inner, Left, Outer | Not required | Combine state |

---

## Stream-Stream Join

```java
KStream<String, String> impressions = builder.stream("impressions");
KStream<String, String> clicks = builder.stream("clicks");

// Must arrive within 5 minutes
JoinWindows window = JoinWindows.ofTimeDifferenceWithNoGrace(
    Duration.ofMinutes(5)
);

KStream<String, String> clickThrough = impressions.join(
    clicks,
    (impression, click) -> impression + "," + click,
    window
);
```

---

## Stream-Table Join (Enrichment)

```java
KStream<String, String> orders = builder.stream("orders");
KTable<String, String> users = builder.table("users");

// Enrich each order with user info
KStream<String, String> enriched = orders.join(
    users,
    (order, user) -> order + ",user=" + user
);
```

**No windowing needed** - table represents current state.

---

# Chapter 05
## Windowing

---

## Window Types

| Type | Size | Overlap | Use Case |
|------|------|---------|----------|
| **Tumbling** | Fixed | No | Hourly metrics |
| **Hopping** | Fixed | Yes | Moving averages |
| **Session** | Variable | No | User sessions |

---

## Tumbling Windows

Non-overlapping, fixed-size:

```java
stream.groupByKey()
    .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5)))
    .count()
```

```
[0-5min] [5-10min] [10-15min] [15-20min] ...
```

---

## Session Windows

Activity-based, variable-size:

```java
stream.groupByKey()
    .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(
        Duration.ofMinutes(5)
    ))
    .count()
```

**Session ends** after 5 minutes of inactivity.

---

# Chapter 06
## Aggregations

---

## Aggregation Types

```java
// COUNT
stream.groupByKey().count()

// REDUCE (same type)
stream.groupByKey().reduce((v1, v2) -> v1 + v2)

// AGGREGATE (different type)
stream.groupByKey().aggregate(
    () -> new Stats(),              // Initializer
    (key, value, agg) -> {          // Adder
        agg.count++;
        agg.sum += value;
        return agg;
    }
)
```

---

## Materialized Views

State stores can be **queried from outside** topology:

```java
// In topology
KTable<String, Long> counts = stream.groupByKey()
    .count(Materialized.as("counts-store"));

// From REST API
ReadOnlyKeyValueStore<String, Long> store = streams.store(
    StoreQueryParameters.fromNameAndType(
        "counts-store",
        QueryableStoreTypes.keyValueStore()
    )
);
Long count = store.get("user123");
```

---

# Chapter 07
## State Stores

---

## State Store Types

| Type | Use Case | Backing |
|------|----------|---------|
| **KeyValueStore** | General aggregations | RocksDB |
| **WindowStore** | Time windows | RocksDB |
| **SessionStore** | Sessions | RocksDB |

**RocksDB** = embedded key-value database (persistent, fast)

---

## Fault Tolerance

1. **Local state** stored in RocksDB
2. **Changelog topic** replicates changes to Kafka
3. **On failure**, new instance restores from changelog

```
State Store ──replicates to──> Changelog Topic
     │                               │
     └────restore from───────────────┘
```

---

# Chapter 08
## Topology Design and Testing

---

## Design Best Practices

1. **Minimize repartitioning** (expensive)
2. **Filter early** (reduce data volume)
3. **Name operations** (debugging)
4. **Enable optimization** (production)

```java
props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, 
          StreamsConfig.OPTIMIZE);
```

---

## Testing with TopologyTestDriver

**Fast, deterministic, no Kafka cluster needed:**

```java
TopologyTestDriver testDriver = new TopologyTestDriver(topology, props);

TestInputTopic<String, String> input = 
    testDriver.createInputTopic("input", ...);
TestOutputTopic<String, String> output = 
    testDriver.createOutputTopic("output", ...);

input.pipeInput("key", "value");
assertEquals("expected", output.readValue());
```

---

# Chapter 09
## Exactly-Once Semantics

---

## Processing Guarantees

| Guarantee | Duplicates | Data Loss | Performance |
|-----------|------------|-----------|-------------|
| **At-Most-Once** | No | Possible | Fastest |
| **At-Least-Once** | Possible | No | Fast |
| **Exactly-Once** | No | No | Slower |

---

## Enabling EOS

```java
Properties props = new Properties();
props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, 
          StreamsConfig.EXACTLY_ONCE_V2);  // Recommended
props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 3);
```

**EOS v2** is faster and recommended (requires Kafka 2.5+).

---

## How EOS Works

**Atomic transaction:**

1. Read from input topic
2. Process (including state updates)
3. Write to output topic
4. Commit consumer offsets

**All** complete or **none** - no partial results.

---

# Chapter 10
## KSQL/ksqlDB Integration

---

## KSQL vs Kafka Streams

| Aspect | Kafka Streams | KSQL |
|--------|---------------|------|
| **Language** | Java/Scala | SQL |
| **Learning Curve** | Steep | Easy |
| **Flexibility** | High | Limited |
| **Deployment** | Embedded | Server |

---

## KSQL Basics

```sql
-- Create stream
CREATE STREAM page_views (
    user_id VARCHAR,
    page_url VARCHAR
) WITH (
    KAFKA_TOPIC='page-views',
    VALUE_FORMAT='JSON'
);

-- Push query (streaming)
SELECT * FROM page_views EMIT CHANGES;

-- Aggregation
SELECT user_id, COUNT(*) as views
FROM page_views
GROUP BY user_id
EMIT CHANGES;
```

---

## KSQL Joins

```sql
-- Enrich orders with user info
CREATE STREAM enriched_orders AS
    SELECT 
        o.order_id,
        o.amount,
        u.name as customer_name
    FROM orders o
    LEFT JOIN users u ON o.user_id = u.user_id
    EMIT CHANGES;
```

---

# Best Practices Summary

---

## Development Best Practices

1. **Start simple** - build incrementally
2. **Test with TopologyTestDriver** - fast feedback
3. **Name operations** - easier debugging
4. **Use mapValues()** when possible
5. **Filter early** - reduce processing

---

## Production Best Practices

1. **Enable EOS** for correctness
2. **Set replication factor ≥ 3**
3. **Monitor metrics** (lag, throughput)
4. **Enable topology optimization**
5. **Plan for state size**
6. **Use standby replicas** (reduce recovery time)

---

## State Management

1. **RocksDB** for persistence
2. **Changelog topics** for replication
3. **Compaction** for KTable topics
4. **Interactive queries** for external access
5. **Backup state directories** (optional)

---

# Common Patterns

---

## Pattern: Real-Time Analytics

```java
// Count events per key per 1-minute window
events
    .groupByKey()
    .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
    .count()
    .toStream()
    .to("metrics");
```

---

## Pattern: Stream Enrichment

```java
// Enrich stream with reference data
KStream<String, Order> orders = builder.stream("orders");
GlobalKTable<String, Product> products = builder.globalTable("products");

orders.join(products,
    (orderId, order) -> order.getProductId(),
    (order, product) -> enrichOrder(order, product)
);
```

---

## Pattern: Event Routing

```java
// Route events by type
Map<String, KStream<String, String>> branches = events
    .split()
    .branch((k, v) -> v.startsWith("ERROR"), Named.as("errors"))
    .branch((k, v) -> v.startsWith("WARN"), Named.as("warnings"))
    .branch((k, v) -> v.startsWith("INFO"), Named.as("info"))
    .defaultBranch(Named.as("other"));

branches.get("errors").to("error-topic");
branches.get("warnings").to("warn-topic");
```

---

# Troubleshooting

---

## Common Issues

| Problem | Cause | Solution |
|---------|-------|----------|
| **High lag** | Slow processing | Scale instances, optimize code |
| **Repartitioning** | Key changed | Use mapValues(), avoid selectKey |
| **State too large** | No windowing | Add windowing, increase retention |
| **Duplicates** | At-least-once | Enable EOS |
| **Rebalancing** | Instance failures | Check health, increase replicas |

---

## Debugging Tips

1. **Print topology**: `topology.describe()` (from builder.build())
2. **Enable debug logging**: `StreamsConfig.DEBUG`
3. **Use `peek()`**: Inline logging
4. **Check metrics**: `streams.metrics()`
5. **Test with TopologyTestDriver**: Isolated testing

---

# Demo Time!

---

## Running the Examples

```bash
# Clone repo
cd /Users/ansumanroy/projects/kafka-tutorials

# Start Kafka
make kafka-apache-start

# Run word count
cd streams/chapter_01_introduction
gradle runWordCount

# Run enrichment demo
cd ../chapter_04_joins
gradle runTableJoin

# Start ksqlDB
cd ../chapter_10_ksqldb
docker-compose up -d
```

---

# Key Takeaways

---

## Remember These!

1. **KStream = events**, **KTable = state**
2. **Stateless** (filter, map) vs **Stateful** (aggregate, join)
3. **Windowing** required for stream-stream joins
4. **Co-partitioning** required (except GlobalKTable)
5. **EOS v2** for exactly-once guarantees
6. **TopologyTestDriver** for fast testing
7. **RocksDB + Changelog** = fault tolerance
8. **KSQL** for simple SQL-based processing

---

# Questions?

## Resources

- **Documentation**: https://kafka.apache.org/documentation/streams/
- **Examples**: https://github.com/apache/kafka/tree/trunk/streams/examples
- **Tutorial repo**: /Users/ansumanroy/projects/kafka-tutorials/streams/

---

# Thank You!

## Happy Streaming! 🚀

*Master Kafka Streams from basics to production*
