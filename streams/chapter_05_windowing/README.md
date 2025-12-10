# Chapter 05 - Windowing

Time-based grouping of events in Kafka Streams.

## Window Types

| Type | Size | Overlap | Use Case |
|------|------|---------|----------|
| **Tumbling** | Fixed | No | Hourly sales, daily metrics |
| **Hopping** | Fixed | Yes | Moving averages, trends |
| **Session** | Variable | No | User sessions, activity bursts |
| **Sliding** | Fixed | Continuous | Real-time correlations |

## Tumbling Windows

Non-overlapping, fixed-size windows.

```java
TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5))
// [0-5min] [5-10min] [10-15min] ...
```

## Hopping Windows

Overlapping, fixed-size windows with advance interval.

```java
TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5))
    .advanceBy(Duration.ofMinutes(1))
// [0-5min] [1-6min] [2-7min] ...
```

## Session Windows

Variable-size based on inactivity gap.

```java
SessionWindows.ofInactivityGapWithNoGrace(Duration.ofMinutes(5))
// Session ends after 5min of inactivity
```

## Grace Period

Allow late-arriving records.

```java
TimeWindows.ofSizeAndGrace(
    Duration.ofMinutes(5),  // Window size
    Duration.ofSeconds(30)  // Grace period (accept late records)
)
```

## Key Concepts

- **Event Time**: Timestamp in record
- **Processing Time**: When processed
- **Watermark**: Track progress
- **Late Records**: Arrive after window closed
- **Grace Period**: Extra time to accept late records

**Next**: Chapter 06 - Aggregations
