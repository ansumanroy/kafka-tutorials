## Java Advanced Chapter 01 – Reliability & Delivery Guarantees

This is the **Java equivalent** of the Bash reliability tests from `bash/chapters/adv_chapter_01_reliability/`.

It uses **Gradle** and **JUnit 5** to provide proper integration tests that validate Kafka producer reliability features.

---

## Prerequisites

- **Java 17+** (JDK 17 or higher)
- **Gradle** (or use the wrapper: `./gradlew`)
- Kafka cluster running (MSK or local Docker)
- Environment variables set (from `infra/env.msk` or `infra/env.local`)

---

## Project Structure

```
java/adv_chapter_01_reliability/
├── build.gradle                    # Gradle build configuration
├── settings.gradle                 # Project settings
├── README.md                       # This file
└── src/
    ├── main/java/
    │   └── com/kafkatutorials/reliability/
    │       └── ProducerConfigHelper.java    # Utility to load configs from env
    └── test/java/
        └── com/kafkatutorials/reliability/
            ├── ReliabilityIntegrationTest.java    # Main reliability tests
            └── FailureSimulationTest.java         # Failure scenario tests
```

---

## Setup

### 1. Set Environment Variables

Source your Kafka environment file:

```bash
# For MSK
source infra/env.msk

# Or for local Docker
source infra/env.local
```

The tests will read these environment variables:
- `KAFKA_BOOTSTRAP_SERVERS` (required)
- `KAFKA_SECURITY_PROTOCOL` (optional, default: PLAINTEXT)
- `KAFKA_SASL_MECHANISM` (optional)
- `KAFKA_SASL_USERNAME` (optional)
- `KAFKA_SASL_PASSWORD` (optional)

### 2. Build the Project

```bash
cd java/adv_chapter_01_reliability
./gradlew build
```

---

## Running Tests

### Run All Tests

```bash
./gradlew test
```

### Run Only Reliability Tests

```bash
./gradlew runReliabilityTests
```

This runs tests tagged with `@Tag("reliability")`:
- Test 1: acks=1 (leader only)
- Test 2: acks=all (all replicas)
- Test 3: Idempotence (exactly-once)
- Test 4: Retry behavior
- Test 5: Keyed messages with ordering
- Test 6: Message count verification
- Test 7: Topic health check

### Run Only Failure Simulation Tests

```bash
./gradlew runFailureTests
```

This runs tests tagged with `@Tag("failure-simulation")`:
- Scenario 1: Short timeout simulation
- Scenario 2: Aggressive retries with backoff
- Scenario 3: Fire-and-forget mode (acks=0)
- Scenario 4: Compare acks levels

### Run a Specific Test

```bash
./gradlew test --tests "ReliabilityIntegrationTest.testIdempotence"
```

---

## Test Output

The tests produce detailed console output:

```
[Test 1] Testing acks=1 (Leader only acknowledgement)...
[Test 1] ✓ Sent 10 messages with acks=1

[Test 2] Testing acks=all (All replicas acknowledgement)...
[Test 2] ✓ Sent 10 messages with acks=all

[Test 3] Testing with idempotence enabled...
[Test 3] ✓ Sent 10 idempotent messages

...
```

---

## What Gets Tested

### ReliabilityIntegrationTest

1. **acks=1**: Leader-only acknowledgment (moderate durability)
2. **acks=all**: Wait for all in-sync replicas (strong durability)
3. **Idempotence**: Exactly-once semantics with deduplication
4. **Retries**: High retry configuration with backoff
5. **Keyed Messages**: Ordering guarantees per partition
6. **No Duplicates**: Verification that idempotence prevents duplicates
7. **Topic Health**: Partition and replication status

### FailureSimulationTest

1. **Short Timeout**: Simulates network delays and timeout behavior
2. **Aggressive Retries**: Tests retry mechanism under transient failures
3. **Fire-and-Forget**: Demonstrates risks of acks=0
4. **Acks Comparison**: Performance characteristics of different ack levels

---

## Configuration Examples

The `ProducerConfigHelper` class provides pre-built configurations:

### High Reliability

```java
Properties props = ProducerConfigHelper.getHighReliabilityConfig();
// Sets: idempotence=true, acks=all, retries=MAX, compression=lz4
```

### Balanced

```java
Properties props = ProducerConfigHelper.getBalancedConfig();
// Sets: idempotence=true, acks=all, moderate retries, linger=10ms
```

### Fire-and-Forget

```java
Properties props = ProducerConfigHelper.getFireAndForgetConfig();
// Sets: acks=0, retries=0 (NOT recommended for production)
```

---

## Relationship to Bash Scripts

| Bash Script | Java Test Class | Purpose |
|-------------|-----------------|---------|
| `test_reliability.sh` | `ReliabilityIntegrationTest.java` | Comprehensive reliability tests |
| `simulate_failure.sh` | `FailureSimulationTest.java` | Failure scenario simulations |

The Java tests provide the same coverage as Bash but with:
- ✓ Proper assertions and test framework
- ✓ Better error reporting
- ✓ CI/CD integration support
- ✓ IDE integration (run/debug individual tests)

---

## Continuous Integration

Add to your CI pipeline:

```yaml
# GitHub Actions example
- name: Run Kafka Reliability Tests
  env:
    KAFKA_BOOTSTRAP_SERVERS: ${{ secrets.KAFKA_BOOTSTRAP_SERVERS }}
  run: |
    cd java/adv_chapter_01_reliability
    ./gradlew test
```

---

## Troubleshooting

### Tests Fail with "KAFKA_BOOTSTRAP_SERVERS not set"

Make sure you've sourced the environment file before running tests:

```bash
source infra/env.msk  # or env.local
./gradlew test
```

### Connection Timeout Errors

Check that:
1. Kafka cluster is running and reachable
2. `KAFKA_BOOTSTRAP_SERVERS` is correct
3. Security settings (SASL/TLS) match your cluster

### Topic Already Exists Errors

The tests clean up after themselves, but if interrupted, you may need to manually delete test topics:

```bash
kafka-topics.sh --bootstrap-server $KAFKA_BOOTSTRAP_SERVERS \
  --delete --topic reliability-test-topic-java

kafka-topics.sh --bootstrap-server $KAFKA_BOOTSTRAP_SERVERS \
  --delete --topic failure-test-topic-java
```

---

## References

- [Kafka Producer Configurations](https://kafka.apache.org/documentation/#producerconfigs)
- [JUnit 5 User Guide](https://junit.org/junit5/docs/current/user-guide/)
- [Gradle Testing Guide](https://docs.gradle.org/current/userguide/java_testing.html)

