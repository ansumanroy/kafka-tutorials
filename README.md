## Kafka Tutorials as Code

This repository teaches **Kafka 101** concepts through **executable code**, starting with Bash scripts.

- Primary assumption: you have access to an **AWS MSK** cluster.
- Fallback: you can run Kafka **locally via Docker Compose**.

The audience is developers comfortable with Linux and the command line but **new to Kafka**.

---

## 🚀 Quick Start (Under 2 Minutes!)

```bash
# One command to setup everything
make setup-and-test

# Run your first advanced chapter
make test-adv-ch01

# Open Kafka UI in browser
open http://localhost:8080
```

**👉 See [QUICKSTART.md](QUICKSTART.md) for complete guide with all commands**

---

## Repository Structure

- `bash/` – Bash-based tutorial chapters (current focus).
  - `bash/common/` – Shared helpers (env loading, logging).
  - `bash/chapters/` – One folder per chapter.
- `infra/` – Environment setup.
  - `infra/docker-compose.kafka.yml` – Local Kafka + Zookeeper stack.
  - `infra/env-example.msk` – Example MSK environment file.
  - `infra/env-example.local` – Example local Docker Kafka environment file.
- `python/` – Future Python examples mirroring Bash chapters.
- `java/` – Future Java examples mirroring Bash chapters.
- `TEACHING_GUIDE.md` – Suggestions for structuring teaching sessions with this repo.
- `ADVANCED_PRODUCERS.md` – Best practices and advanced patterns for Kafka producers.

---

## Prerequisites

- Unix-like shell (tested on macOS/Linux).
- Kafka CLI tools on your `PATH` (`kafka-topics.sh`, `kafka-console-producer.sh`, `kafka-console-consumer.sh`, `kafka-consumer-groups.sh`).
- Either:
  - An **AWS MSK** cluster reachable from your machine, or
  - Docker + Docker Compose for running Kafka locally.

Optional tools:

- `jq` for pretty-printing JSON in serialization chapters.

---

## Quickstart

### 1. Configure Environment

For **MSK**:

1. Copy the example file:
   - `cp infra/env-example.msk infra/env.msk`
2. Edit `infra/env.msk` and set:
   - `KAFKA_BOOTSTRAP_SERVERS` to your MSK bootstrap servers.
   - Security-related variables (`KAFKA_SECURITY_PROTOCOL`, `KAFKA_SASL_*`) according to your setup.

For **local Docker Kafka**:

1. Copy the example file:
   - `cp infra/env-example.local infra/env.local`
2. Start Kafka locally:

   ```bash
   bash/setup/docker_start_kafka.sh
   ```

3. Optionally export the printed `KAFKA_BOOTSTRAP_SERVERS` in your shell.

### 2. Test Connectivity (Chapter 01)

```bash
bash/chapters/01-environment/check_connection.sh
```

If you see a success message, you are ready for the rest of the chapters.

### 3. Create Your First Topic (Chapter 02)

```bash
bash/chapters/02-topics/create_topic.sh my-first-topic
bash/chapters/02-topics/list_topics.sh
```

Then continue through the chapters in order.

---

## Makefile Targets

The repository includes a `Makefile` with convenient targets for running tests across Python and Java:

### Quick Reference

```bash
# Show all available targets
make help

# Python targets
make python-env                    # Create Python virtual environment
make python-check-connection       # Run Python connectivity check

# Java targets
make java-reliability-build        # Build Java reliability tests
make java-reliability-test         # Run all Java reliability tests
make java-reliability-integration  # Run only integration tests
make java-reliability-failure      # Run only failure simulation tests
make java-reliability-clean        # Clean Java build artifacts

# Run everything
make test-all                      # Run all tests (Python + Java)
make clean-all                     # Clean all build artifacts
```

**Note**: Always source your environment file before running tests:

```bash
source infra/env.msk  # or infra/env.local
make java-reliability-test
```

---

## Chapter Index (Bash)

| #  | Chapter                                   | Folder                                      |
|----|-------------------------------------------|---------------------------------------------|
| 01 | Environment & Connectivity                | `bash/chapters/01-environment`              |
| 02 | Topics Basics                             | `bash/chapters/02-topics`                   |
| 03 | Producers 101                             | `bash/chapters/03-producer-console`         |
| 04 | Consumers 101                             | `bash/chapters/04-consumer-console`         |
| 05 | Partitions & Offsets                      | `bash/chapters/05-partitions-offsets`       |
| 06 | Consumer Groups                           | `bash/chapters/06-consumer-groups`          |
| 07 | Serialization Basics                      | `bash/chapters/07-serialization`            |
| 08 | Troubleshooting & Debugging               | `bash/chapters/08-troubleshooting`          |

Each chapter directory contains a `README.md` explaining the concepts and one or more scripts you can run directly.

For suggested **multi-session teaching flows**, see `TEACHING_GUIDE.md`.

For **advanced producer concepts** and best practices, see `ADVANCED_PRODUCERS.md`.

---

## Future: Python and Java

The `python/` and `java/` directories will eventually contain **equivalent examples** using Kafka client libraries:

- Python: e.g. `confluent-kafka` or `kafka-python`.
- Java: official Kafka client and/or Spring Kafka.

They will follow the same chapter numbering so you can switch between languages while keeping the conceptual flow.
