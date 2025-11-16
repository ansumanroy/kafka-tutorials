# Chapter 03 - Producers 101

A **producer** sends messages to a Kafka topic. Producers choose:

- Which topic to send to.
- Optionally a key (used for partitioning).
- Delivery guarantees (e.g. `acks`).

In this chapter we use the `kafka-console-producer.sh` CLI.

## Scripts

### Interactive producer

```bash
bash/chapters/03-producer-console/send_messages.sh my-first-topic
```

This script:

- Loads Kafka connection settings from your env file.
- Starts `kafka-console-producer.sh` against the given topic.
- Lets you type messages line by line; press `Ctrl+D` to finish.

You can control acknowledgements using the `KAFKA_ACKS` environment variable, e.g.:

```bash
KAFKA_ACKS=all bash/chapters/03-producer-console/send_messages.sh my-first-topic
```

### Batch producer

```bash
bash/chapters/03-producer-console/send_batch_messages.sh my-first-topic 20
```

This sends a fixed sequence of messages (`message-1`, `message-2`, ...).

Parameters:

- `<topic-name>`: target topic.
- `count` (optional): how many messages to send (default: 10).

You can also set `KAFKA_ACKS` for this script as above.

## Concept Recap

- Producers are **clients that publish messages** to topics.
- The `acks` setting controls how many brokers must confirm before the send is considered successful:
  - `0` = fire-and-forget.
  - `1` = leader only.
  - `all` = all in-sync replicas.
