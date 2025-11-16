# Chapter 05 - Partitions & Offsets

Each Kafka topic is split into **partitions**. Within a partition, messages are strictly ordered by **offset**.

This chapter demonstrates how keys affect partitioning and how to inspect offsets.

## Scripts

### Produce keyed messages

```bash
bash/chapters/05-partitions-offsets/produce_with_keys.sh my-first-topic
```

This sends messages with keys like `key-0`, `key-1`, `key-2`. Messages with the same key go to the same partition.

You can later inspect which partitions the messages landed in.

### Show partition offsets (and consumer lag)

```bash
bash/chapters/05-partitions-offsets/show_offsets.sh my-first-topic
```

This uses `kafka-topics.sh --describe` to show:

- Partition IDs.
- Leaders and replicas.
- Log end offsets.

If you have a consumer group (see Chapter 06), you can also show its lag:

```bash
bash/chapters/05-partitions-offsets/show_offsets.sh my-first-topic my-consumer-group
```

## Concept Recap

- Partitions allow Kafka to scale horizontally while preserving order **within each partition**.
- Offsets are monotonically increasing numbers assigned to each message.
- Keys control which partition a message goes to (for producers that use keys).
