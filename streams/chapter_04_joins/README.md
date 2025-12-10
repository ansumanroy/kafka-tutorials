# Chapter 04 - Joins

Kafka Streams supports three types of joins: stream-stream, stream-table, and table-table.

## Join Types Matrix

| Left | Right | Join Types | Windowing | Use Case |
|------|-------|------------|-----------|----------|
| **KStream** | **KStream** | Inner, Left, Outer | Required | Correlate events |
| **KStream** | **KTable** | Inner, Left | Not required | Enrich stream |
| **KStream** | **GlobalKTable** | Inner, Left | Not required | Enrich (no co-partitioning) |
| **KTable** | **KTable** | Inner, Left, Outer | Not required | Combine state |

## Stream-Stream Joins

**Windowed joins**: Records must arrive within a time window.

```java
KStream<String, String> impressions = builder.stream("impressions");
KStream<String, String> clicks = builder.stream("clicks");

// Join if click within 5 minutes of impression
JoinWindows window = JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(5));

KStream<String, String> clickThrough = impressions.join(
    clicks,
    (impression, click) -> impression + "," + click,
    window
);
```

**Key requirement:** Both streams must have same key for joining.

## Stream-Table Joins

**Enrichment pattern**: Lookup latest table value for each stream record.

```java
KStream<String, String> orders = builder.stream("orders");
KTable<String, String> users = builder.table("users");

// Enrich each order with user info (current state)
KStream<String, String> enriched = orders.join(
    users,
    (order, user) -> order + ",user=" + user
);
```

**No windowing needed** - table represents current state.

## Table-Table Joins

**State join**: Combine current state from two tables.

```java
KTable<String, String> profiles = builder.table("profiles");
KTable<String, String> preferences = builder.table("preferences");

KTable<String, String> complete = profiles.join(
    preferences,
    (profile, prefs) -> profile + "," + prefs
);
```

## Co-Partitioning Requirement

For stream-stream and stream-table joins, **both sides must be co-partitioned**:

1. Same number of partitions
2. Same partitioning strategy
3. Keys must hash to same partition

**Solution for different keys:**

```java
// Repartition to match keys
stream.selectKey((k, v) -> extractUserId(v))  // Change key
      .join(table, ...)                        // Now can join
```

**GlobalKTable avoids this** - fully replicated, no co-partitioning needed.

## Running Examples

```bash
# Create topics
kafka-topics.sh --create --topic ad-impressions --partitions 1 ...
kafka-topics.sh --create --topic ad-clicks --partitions 1 ...

# Run demo
gradle runStreamJoin

# Test
gradle test
```

## Key Takeaways

1. **Stream-stream** requires windowing
2. **Stream-table** for enrichment (no windowing)
3. **Table-table** for state combination
4. **Co-partitioning** required (except GlobalKTable)
5. **Inner join** = only matches
6. **Left join** = all left, nulls for unmatched right

**Next**: Chapter 05 - Windowing
