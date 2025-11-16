# Chapter 06 - Consumer Groups

A **consumer group** is a set of consumers that cooperate to read from a topic.

- Each partition is consumed by at most **one** member of the group.
- Adding consumers can increase parallelism.
- Kafka tracks offsets per **group id**.

## Scripts

### Start a group consumer

```bash
bash/chapters/06-consumer-groups/start_group_consumer.sh my-first-topic my-group
```

Start this in multiple terminals (same `group-id`) to see partitions assigned across consumers and rebalancing when you start/stop consumers.

### Describe a consumer group

```bash
bash/chapters/06-consumer-groups/describe_group.sh my-group
```

This uses `kafka-consumer-groups.sh --describe` to show:

- Which consumer instance owns which partitions.
- Current offsets.
- Lag for each partition.

## Concept Recap

- Consumer groups let you scale out message processing.
- Offsets are stored per group so different applications can consume the same topic independently.
