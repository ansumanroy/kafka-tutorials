# Chapter 02 - Topics Basics

In Kafka, **topics** are named feeds of messages. Each topic is split into **partitions**, which are ordered, append-only logs.

This chapter focuses on basic topic operations:

- Creating a topic.
- Listing topics.
- Deleting a topic safely.

All scripts rely on the environment from Chapter 01.

## Scripts

### Create a topic

```bash
bash/chapters/02-topics/create_topic.sh my-first-topic
```

Environment variables (optional):

- `PARTITIONS` (default: `1`)
- `REPLICATION_FACTOR` (default: `1`)

Example:

```bash
PARTITIONS=3 REPLICATION_FACTOR=2 bash/chapters/02-topics/create_topic.sh orders
```

The script is **idempotent**: if the topic already exists, it prints a warning and exits successfully.

### List topics

```bash
bash/chapters/02-topics/list_topics.sh
```

This calls `kafka-topics.sh --list` against your configured cluster.

### Delete a topic (careful!)

```bash
bash/chapters/02-topics/delete_topic.sh my-first-topic --force
```

Without `--force`, the script will refuse to delete the topic and will remind you of the irreversible nature of deletion.

## Concept Recap

- A **topic** is a category or feed name used to store messages.
- A **partition** is an ordered, immutable sequence of messages within a topic.
- The **replication factor** controls how many brokers copy the data for fault tolerance.
