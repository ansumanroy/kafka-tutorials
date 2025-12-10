# Chapter 07 - State Stores

Local storage for stateful operations in Kafka Streams.

## Store Types

| Type | Use Case | Backing |
|------|----------|---------|
| **KeyValueStore** | Aggregations, lookups | RocksDB |
| **WindowStore** | Time-windowed aggregations | RocksDB |
| **SessionStore** | Session-based aggregations | RocksDB |
| **Custom** | Specialized needs | Custom |

## KeyValueStore

Most common store type. Created automatically by aggregations.

```java
stream.groupByKey()
    .count(Materialized.as("my-store"))  // Creates KeyValueStore
```

## Persistence

**Persistent stores** (default):
- Backed by RocksDB
- Survives restarts
- Changelog topic for replication

**In-memory stores**:
- Faster but lost on restart
- Still has changelog for fault tolerance

```java
Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("store")
    .withLoggingDisabled()  // No changelog (risky!)
    .withCachingDisabled()  // No caching
```

## Interactive Queries

Query stores from outside topology (e.g., REST API).

```java
ReadOnlyKeyValueStore<String, Long> store = streams.store(
    StoreQueryParameters.fromNameAndType(
        "my-store",
        QueryableStoreTypes.keyValueStore()
    )
);

Long value = store.get("key");
```

## Changelog Topics

Kafka Streams automatically creates changelog topics:
- Format: `{application-id}-{store-name}-changelog`
- Always compacted
- Used for state restoration on failure

## Fault Tolerance

1. **State replicated** to changelog topic
2. **On failure**, new instance restores from changelog
3. **Standby replicas** can reduce recovery time

## Key Concepts

- **State stores** = local, fast storage
- **Changelog** = replicated, durable backup
- **RocksDB** = embedded key-value database
- **Interactive queries** = query from outside topology
- **Restoration** = rebuild state from changelog

**Next**: Chapter 08 - Topology Design

