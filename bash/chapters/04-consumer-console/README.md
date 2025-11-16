# Chapter 04 - Consumers 101

A **consumer** reads messages from a Kafka topic. Consumers track their **offsets** (positions) in each partition.

In this chapter we use the `kafka-console-consumer.sh` CLI.

## Scripts

### Consume messages

```bash
bash/chapters/04-consumer-console/consume_messages.sh my-first-topic
```

By default this starts at the **latest** offset, meaning it will only see new messages.

To read **all existing messages** from the beginning:

```bash
bash/chapters/04-consumer-console/consume_messages.sh my-first-topic earliest
```

Press `Ctrl+C` to stop the consumer.

## Concept Recap

- An **offset** is like a line number in the partition log.
- Consumers can start at the earliest or latest offset depending on how you configure them.
- Later chapters introduce **consumer groups** and how offsets are tracked per group.
