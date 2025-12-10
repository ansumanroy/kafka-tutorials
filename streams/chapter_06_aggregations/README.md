# Chapter 06 - Aggregations

Stateful operations that combine multiple records into summary values.

## Aggregation Types

| Operation | Description | Example |
|-----------|-------------|---------|
| `count()` | Count records per key | Page views per user |
| `reduce()` | Combine values (same type) | Sum, max, concatenate |
| `aggregate()` | Custom aggregation (different types) | Statistics, histograms |

## Count

```java
stream.groupByKey()
    .count(Materialized.as("counts"))
    .toStream()
    .to("output");
```

## Reduce

Combines two values into one (same type).

```java
stream.groupByKey()
    .reduce((value1, value2) -> value1 + value2)
```

## Aggregate

Most flexible - can change type and maintain complex state.

```java
stream.groupByKey()
    .aggregate(
        () -> new Stats(),              // Initializer
        (key, value, agg) -> {          // Adder
            agg.count++;
            agg.sum += value;
            return agg;
        },
        Materialized.as("stats")
    )
```

## Materialized Views

State stores can be queried from outside topology.

```java
ReadOnlyKeyValueStore<String, Long> store = streams.store(
    StoreQueryParameters.fromNameAndType(
        "counts",
        QueryableStoreTypes.keyValueStore()
    )
);
Long count = store.get("user123");
```

**Use cases:** REST APIs, dashboards, health checks.

**Next**: Chapter 07 - State Stores
