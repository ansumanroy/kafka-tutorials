# Chapter 01 - Environment & Connectivity

This chapter helps you get a Kafka cluster and CLI set up so that later chapters can focus on concepts.

We assume **AWS MSK** as the primary cluster, with a **Docker Compose** Kafka as a local fallback.

## Prerequisites

- Kafka CLI tools available on your `PATH` (e.g. `kafka-topics.sh`, `kafka-console-producer.sh`, etc.).
- Either:
  - An **AWS MSK** cluster you can reach from your environment, or
  - Docker + Docker Compose for running Kafka locally.

## Step 1: Configure Environment

For **MSK**:

1. Copy the example file:
   - `cp infra/env-example.msk infra/env.msk`
2. Edit `infra/env.msk` and set:
   - `KAFKA_BOOTSTRAP_SERVERS` to your MSK bootstrap servers.
   - Security-related variables (`KAFKA_SECURITY_PROTOCOL`, `KAFKA_SASL_*`) as needed.

For **local Docker Kafka**:

1. Copy the example file:
   - `cp infra/env-example.local infra/env.local`
2. Adjust values if you changed ports or host names in `infra/docker-compose.kafka.yml`.

## Step 2: (Optional) Start Local Kafka via Docker

If you don't have MSK access, run:

```bash
bash/setup/docker_start_kafka.sh
```

This script starts Kafka using Docker Compose and prints an `export KAFKA_BOOTSTRAP_SERVERS=...` line you can use in your shell.

## Step 3: Test Connectivity

Use the chapter script:

```bash
bash/chapters/01-environment/check_connection.sh
```

This script:

- Loads the appropriate env file via `bash/common/env.sh`.
- Verifies basic CLI availability.
- Uses `kafka-topics.sh --list` to confirm it can talk to the cluster.

If it prints **"Successfully connected"**, you're ready for the next chapters.
