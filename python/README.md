## Python Kafka Examples (Coming Soon)

This directory will contain **Python implementations** of the same chapters provided in `bash/chapters/`.

Planned approach:

- Use a Kafka client library such as `confluent-kafka` or `kafka-python`.
- Mirror the Bash chapter structure:
  - Chapter 01: connectivity checks.
  - Chapter 02: topic management.
  - Chapter 03: producers.
  - Chapter 04: consumers.
  - Chapter 05: partitions & offsets.
  - Chapter 06: consumer groups.
  - Chapter 07: serialization.
  - Chapter 08: troubleshooting patterns.

The Python examples will read the **same environment files** under `infra/` so you can switch between Bash, Python, and Java without changing cluster config.
