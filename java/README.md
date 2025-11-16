## Java Kafka Examples (Coming Soon)

This directory will contain **Java implementations** of the Kafka 101 chapters.

Planned approach:

- Use the official Kafka Java client (and possibly Spring Kafka for higher-level examples).
- Mirror the Bash chapter structure so each concept has a Java counterpart:
  - Chapter 01: connectivity.
  - Chapter 02: topic management.
  - Chapter 03: producers.
  - Chapter 04: consumers.
  - Chapter 05: partitions & offsets.
  - Chapter 06: consumer groups.
  - Chapter 07: serialization.
  - Chapter 08: troubleshooting patterns.

Like the Bash and Python examples, Java code will read connection details from the env files in `infra/` so you can point all languages at the same Kafka cluster (MSK or local Docker).
