## Java Kafka Examples

This directory contains **Java implementations** using the official Kafka Java client with proper integration tests.

### Available Chapters

#### Advanced Chapters

- **[adv_chapter_01_reliability](adv_chapter_01_reliability/)** - Producer reliability and delivery guarantees
  - JUnit 5 integration tests
  - Gradle build system
  - Tests for idempotence, acks levels, retries, ordering, and failure scenarios
  - Mirrors `bash/chapters/adv_chapter_01_reliability/` functionality

### Planned Basic Chapters

These will mirror the Bash chapter structure:
  - Chapter 01: connectivity.
  - Chapter 02: topic management.
  - Chapter 03: producers.
  - Chapter 04: consumers.
  - Chapter 05: partitions & offsets.
  - Chapter 06: consumer groups.
  - Chapter 07: serialization.
  - Chapter 08: troubleshooting patterns.

### Approach

- Use the official Kafka Java client (and possibly Spring Kafka for higher-level examples).
- Java code reads connection details from the env files in `infra/` so you can point all languages at the same Kafka cluster (MSK or local Docker).
- Integration tests use JUnit 5 with proper assertions and CI/CD support.

### Running Java Tests

```bash
# Navigate to a chapter
cd java/adv_chapter_01_reliability

# Source environment
source ../../infra/env.msk  # or env.local

# Run all tests
./gradlew test

# Or if gradle is installed globally
gradle test
```
