## Kafka 101 Teaching Guide

This guide is for instructors using this repository to teach Kafka 101.
It assumes learners are comfortable with Linux but new to Kafka.

---

## General Tips

- **Keep terminals visible**: One for infra (Docker/MSK), one for producers, one for consumers.
- **Type, don’t paste**: Have learners type core commands to build muscle memory.
- **Explain before running**: Describe what a script does and what success/failure looks like.
- **Relate to real systems**: Map topics/partitions/groups to services learners know.

Suggested pacing: **1–2 chapters per 60–90 minute session**.

---

## Session 1 – Foundations (Chapters 01–02)

### Objectives

- Understand what a Kafka cluster is.
- Be able to connect to MSK or local Docker Kafka.
- Create, list, and delete topics.

### Flow

1. **Concept intro (10–15 min)**
   - Brokers, topics, partitions at a high level.
   - MSK vs local Kafka; why the repo supports both.
2. **Environment setup (20–30 min)**
   - Walk through `infra/env-example.msk` and `infra/env-example.local`.
   - Demo `bash/setup/docker_start_kafka.sh` for local use.
   - Run `bash/chapters/01-environment/check_connection.sh`.
   - Discuss typical connection/auth failures and how to read error messages.
3. **Topics basics (20–30 min)**
   - Show `bash/chapters/02-topics/README.md`.
   - Hands-on:
     - Create a topic: `create_topic.sh my-first-topic`.
     - List topics: `list_topics.sh`.
     - Try creating the same topic again and note idempotent behavior.
     - Delete a topic with and without `--force`.
4. **Wrap-up (5–10 min)**
   - Ask: What is a topic? Why might you use multiple topics?
   - Preview next session: producers and consumers.

---

## Session 2 – Producers & Consumers (Chapters 03–04)

### Objectives

- Understand producers and consumers at a basic level.
- Send and read messages from a topic.

### Flow

1. **Concept intro (10–15 min)**
   - What is a producer? What is a consumer?
   - High-level view of offsets and durability.
2. **Producers (20–30 min)**
   - Use `bash/chapters/03-producer-console/README.md`.
   - Demo interactive producer:
     - `send_messages.sh my-first-topic`.
     - Show `KAFKA_ACKS` variations (`1`, `all`) and discuss reliability trade-offs.
   - Batch producer:
     - `send_batch_messages.sh my-first-topic 20` to quickly generate data.
3. **Consumers (20–30 min)**
   - Use `bash/chapters/04-consumer-console/README.md`.
   - Read new messages: `consume_messages.sh my-first-topic` (default latest).
   - Read from the beginning: `consume_messages.sh my-first-topic earliest`.
   - Emphasize the idea of **offset** as position in the log.
4. **Wrap-up (5–10 min)**
   - Ask: How does `earliest` vs `latest` change what you see?
   - Preview: partitions, scaling, and consumer groups.

---

## Session 3 – Partitions & Consumer Groups (Chapters 05–06)

### Objectives

- Understand partitions and ordering guarantees.
- See how consumer groups scale reads and how rebalancing works.

### Flow

1. **Concept intro (10–15 min)**
   - Partitions as parallel logs.
   - Ordering **within** a partition but not across all partitions.
2. **Partitions with keys (20–25 min)**
   - Use `bash/chapters/05-partitions-offsets/README.md`.
   - Create a topic with multiple partitions (if not already done).
   - Run `produce_with_keys.sh my-first-topic`.
   - Use `show_offsets.sh` to inspect partitions and offsets.
   - Discuss how keys influence which partition a message goes to.
3. **Consumer groups (20–30 min)**
   - Use `bash/chapters/06-consumer-groups/README.md`.
   - Start one consumer group member:
     - `start_group_consumer.sh my-first-topic my-group`.
   - Start a second member in another terminal; observe partition assignment and rebalancing.
   - Use `describe_group.sh my-group` and `show_offsets.sh my-first-topic my-group` to show lag.
4. **Wrap-up (5–10 min)**
   - Ask: How would you scale processing when traffic increases?
   - Preview: serialization and debugging real-world issues.

---

## Session 4 – Serialization & Troubleshooting (Chapters 07–08)

### Objectives

- Understand that Kafka treats data as bytes and why serialization matters.
- Learn basic troubleshooting techniques for topics and consumers.

### Flow

1. **Serialization basics (20–25 min)**
   - Use `bash/chapters/07-serialization/README.md`.
   - Send JSON messages: `send_json_messages.sh my-json-topic 5`.
   - Consume with pretty-printing: `consume_json_messages.sh my-json-topic`.
   - Discuss:
     - Kafka is schema-agnostic.
     - Why teams pick JSON vs Avro vs Protobuf, etc.
2. **Troubleshooting (20–30 min)**
   - Use `bash/chapters/08-troubleshooting/README.md`.
   - Demonstrate `check_topic_health.sh` and explain leader/replica fields.
   - Demonstrate `check_consumer_lag.sh my-group` and talk about lag.
   - Walk through a few common failure scenarios:
     - Wrong bootstrap servers.
     - Topic missing.
     - Consumer group not reading (stopped consumers).
3. **Wrap-up (10–15 min)**
   - Recap: topics, partitions, producers, consumers, groups, serialization.
   - Ask learners to describe a simple system they could build with Kafka.
   - Point to `python/` and `java/` READMEs for language-specific follow-ups.

---

## Suggested Exercises

Use these as homework or in-class practice:

- **Exercise A**: Create separate `orders` and `payments` topics and discuss which service would produce/consume each.
- **Exercise B**: Create a topic with 4 partitions and design a keying strategy so all messages for a given user go to the same partition.
- **Exercise C**: Run a consumer group with 3 members on a topic with 2 partitions; observe and explain the partition assignments.
- **Exercise D**: Intentionally misconfigure `KAFKA_BOOTSTRAP_SERVERS` and have learners debug using the troubleshooting scripts.
- **Exercise E**: Extend the JSON payloads with nested fields and have learners write filters using `jq` on the consumed output.

This structure should give you **4 sessions** that build up from “what is Kafka” to “I can operate a basic Kafka-backed system and reason about its behavior.”
