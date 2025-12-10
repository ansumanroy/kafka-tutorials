# Chapter 09 - Exactly-Once Semantics

Kafka Streams supports exactly-once processing semantics (EOS), ensuring no duplicates and no data loss.

## Processing Guarantees

| Guarantee | Duplicates on Failure | Data Loss | Performance |
|-----------|----------------------|-----------|-------------|
| **At-Most-Once** | No | Possible | Fastest |
| **At-Least-Once** | Possible | No | Fast |
| **Exactly-Once** | No | No | Slower |

## Enabling Exactly-Once

```java
Properties props = new Properties();
props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 3);  // Production
```

## How EOS Works

### Atomic Operations

With EOS, the following are atomic:
1. Read from input topic
2. Process (including state updates)
3. Write to output topic
4. Commit consumer offsets

Either **all** complete or **none** - no partial results.

### Transactions

```
Begin Transaction
  ├─ Read message from input topic
  ├─ Update state store
  ├─ Write result to output topic
  └─ Commit consumer offset
Commit Transaction
```

If any step fails → transaction aborts → nothing persisted.

## EOS v1 vs EOS v2

| Feature | EOS v1 | EOS v2 (Recommended) |
|---------|---------|---------------------|
| Transactional Producer | Per task | Per thread |
| Zombie Fencing | Slower | Faster |
| Performance | Good | Better |
| Broker Version | 0.11+ | 2.5+ |

**Always use EOS v2** unless on old Kafka.

## Configuration

### Required

```java
props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
```

### Recommended

```java
props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 3);
props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10000);  // 10 seconds
```

### Broker Requirements

```properties
# Broker config (server.properties)
transaction.state.log.replication.factor=3
transaction.state.log.min.isr=2
```

## Performance Impact

EOS adds overhead:
- ~10-20% lower throughput
- Higher latency due to transactions
- More CPU/memory for coordination

**Trade-off**: Correctness vs performance.

## When to Use EOS

### ✅ Use Exactly-Once When:
- Correctness is critical (financial, billing)
- Duplicates unacceptable
- End-to-end exactly-once needed

### ⚠️ Use At-Least-Once When:
- Performance is critical
- Duplicates acceptable or handled downstream
- Simpler deployment preferred

## Zombie Fencing

**Zombie instances** = old instances that didn't shut down cleanly.

EOS prevents zombies from corrupting state:
- Each instance gets unique transaction ID
- Broker tracks active IDs
- Old IDs are "fenced out"

## Testing EOS

**Note**: TopologyTestDriver doesn't simulate failures, so full EOS behavior can't be tested in unit tests.

For EOS testing:
1. Integration tests with real Kafka
2. Chaos engineering (kill instances)
3. Verify no duplicates after restarts

## Key Takeaways

1. **EOS = no duplicates, no data loss**
2. **EOS v2 recommended** (faster, better)
3. **Atomic**: read-process-write-commit
4. **Transactions** ensure all-or-nothing
5. **Trade-off**: correctness vs performance
6. **Broker config** required for production
7. **Testing** needs integration tests

**Next**: Chapter 10 - KSQL/ksqlDB Integration
