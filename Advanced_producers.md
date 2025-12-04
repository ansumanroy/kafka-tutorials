## Advanced Kafka Producer Guide

This document outlines best practices for Kafka producers and can be used as the basis for `adv_chapter_*` examples.

---

## Advanced Chapters (Producers)

These are planned `adv_chapter_*` folders focusing on Kafka producer best practices.

| #   | Advanced Chapter                                        | Suggested Folder                               | Focus                                                                 |
|-----|---------------------------------------------------------|-----------------------------------------------|----------------------------------------------------------------------|
| A01 | Reliability & Delivery Guarantees                       | `bash/chapters/adv_chapter_01_reliability`    | Idempotence, acks, retries, timeouts                                |
| A02 | Performance & Throughput                               | `bash/chapters/adv_chapter_02_performance`    | Batching, linger, compression, producer reuse                       |
| A03 | Keys, Partitioning, and Ordering                       | `bash/chapters/adv_chapter_03_partitioning`   | Key choice, hot partitions, ordering guarantees                     |
| A04 | Serialization & Schema Management                      | `bash/chapters/adv_chapter_04_serialization`  | Explicit serializers, schema registry, compatibility                |
| A05 | Error Handling & Observability                         | `bash/chapters/adv_chapter_05_observability`  | Send failures, callbacks, DLQs, metrics, logs, tracing              |
| A06 | Operational Concerns                                   | `bash/chapters/adv_chapter_06_operations`     | Graceful shutdown, limits, security (TLS/SASL, credentials)         |

---

## A01 – Reliability & Delivery Guarantees

- **Idempotent producers**  
  - Enable `enable.idempotence=true` to avoid duplicates on retries.  
  - Combine with strong `acks` and retries.

- **Acks and retries**  
  - Use `acks=all` for strong durability.  
  - Configure `retries` and `retry.backoff.ms`.  
  - For strict ordering, use `max.in.flight.requests.per.connection=1` together with idempotence.

- **Timeouts**  
  - Tune `request.timeout.ms`, `delivery.timeout.ms`, and `linger.ms` to match SLAs and network conditions.

---

## A02 – Performance & Throughput

- **Batching & linger**  
  - Increase `batch.size` and use a small non-zero `linger.ms` (for example 5–20 ms) to improve throughput.

- **Compression**  
  - Enable `compression.type` (`lz4`, `snappy`, or `zstd`) to reduce network and disk usage.

- **Reuse producer instances**  
  - Reuse a producer per process/service instead of creating one per request.

---

## A03 – Keys, Partitioning, and Ordering

- **Deliberate key choice**  
  - Use keys to keep related messages (per user/order/etc.) on the same partition.  
  - Avoid hot-spot keys that push most traffic to a single partition.

- **Ordering scope**  
  - Kafka guarantees ordering **within a partition**, not across the whole topic.  
  - Design keys and partition counts accordingly if ordering matters.

---

## A04 – Serialization & Schema Management

- **Explicit serializers**  
  - Configure key/value serializers explicitly (String, JSON, Avro, Protobuf, etc.).

- **Schemas & compatibility**  
  - Use schema-based formats with a schema registry (Avro/Protobuf/JSON Schema).  
  - Enforce compatibility (backward/forward) to avoid breaking consumers.

---

## A05 – Error Handling & Observability

- **Handle send failures**  
  - For async sends, always check callbacks for exceptions.  
  - For sync sends, catch and log/alert on producer exceptions (timeouts, record too large, authorization errors, etc.).

- **Dead-letter topics**  
  - Route invalid or repeatedly failing messages to a dead-letter topic with enough metadata to debug later.

- **Metrics & tracing**  
  - Expose producer metrics (latency, error rate, retry count, batch sizes).  
  - Add structured logs and tracing around produce calls.

---

## A06 – Operational Concerns

- **Graceful shutdown**  
  - Flush and close producers in shutdown hooks.

- **Limits & protection**  
  - Set reasonable `max.request.size`, `buffer.memory`, and coordinate with broker `message.max.bytes`.

- **Security**  
  - Use TLS/SASL as required (e.g. MSK).  
  - Manage credentials via environment variables or secret managers, not hardcoded values.