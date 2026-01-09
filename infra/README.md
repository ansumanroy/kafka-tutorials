# Infrastructure Setup

This directory contains infrastructure configurations for running Kafka locally or connecting to AWS MSK.

## Files

- `docker-compose-full.yml` - Complete local stack (Kafka + Schema Registry + UI)
- `docker-compose.kafka.yml` - Simple Kafka + Zookeeper only
- `docker-compose-kafkaui.yml` - Kafka UI stack with Redpanda (supports EC2 deployment with SASL)
- `env-example.msk` - Example configuration for AWS MSK
- `env-example.local` - Example configuration for local Docker Kafka
- `README-EC2-DEPLOYMENT.md` - Guide for deploying on EC2 with external access

## Quick Start (Local Development)

### Option 1: Full Stack (Recommended)

Includes Kafka, Schema Registry, and Kafka UI for visual management:

```bash
# From repo root
make kafka-start

# Copy and source environment
cp infra/env-example.local infra/env.local
source infra/env.local

# Verify connection
bash/chapters/01-environment/check_connection.sh
```

Access points:
- **Kafka**: `localhost:9092`
- **Schema Registry**: `http://localhost:8081`
- **Kafka UI**: `http://localhost:8080` (browse topics, messages, schemas in your browser)

### Option 2: Simple Kafka Only

Just Kafka + Zookeeper without additional services:

```bash
make kafka-start-simple
```

## Available Services

### Full Stack (`docker-compose-full.yml`)

| Service | Port | Purpose |
|---------|------|---------|
| Kafka | 9092 | Message broker (PLAINTEXT) |
| Zookeeper | 2181 | Kafka coordination |
| Schema Registry | 8081 | Avro/Protobuf schema management |
| Kafka UI | 8080 | Web interface for Kafka management |

### Architecture

```
┌─────────────────────────────────────────┐
│  Kafka UI (localhost:8080)              │
│  - Browse topics                        │
│  - View messages                        │
│  - Manage schemas                       │
│  - Monitor consumer groups              │
└──────────────┬──────────────────────────┘
               │
    ┌──────────┴──────────┐
    │                     │
┌───▼──────────┐    ┌────▼─────────────────┐
│ Kafka        │    │ Schema Registry      │
│ localhost:   │◄───┤ localhost:8081       │
│   9092       │    │                      │
└───┬──────────┘    └──────────────────────┘
    │
┌───▼──────────┐
│ Zookeeper    │
│ localhost:   │
│   2181       │
└──────────────┘
```

All services use **PLAINTEXT** (no authentication) for local development.

## Management Commands

### For Kafka UI Stack (`docker-compose-kafkaui.yml`)

See the `Makefile` in this directory for convenient commands:

```bash
cd infra

# Install Docker and Docker Compose
make install-docker

# Start Kafka UI stack
make start

# Stop services
make stop

# View status
make status

# View logs
make logs

# Clean everything (requires CONFIRM=true)
make clean CONFIRM=true
```

See `infra/Makefile` for all available targets.

### For Full Stack (`docker-compose-full.yml`)

```bash
# Start full stack
make kafka-start

# Stop (keeps data)
make kafka-stop

# Stop and remove all data
make kafka-stop-all

# Restart
make kafka-restart
```

### Monitoring

```bash
# Check status
make kafka-status

# View all logs
make kafka-logs

# View Kafka logs only
make kafka-logs-kafka

# View Schema Registry logs only
make kafka-logs-schema-registry
```

### Direct Docker Compose Commands

```bash
# Start in foreground (see logs live)
docker compose -f infra/docker-compose-full.yml up

# Start in background
docker compose -f infra/docker-compose-full.yml up -d

# Stop
docker compose -f infra/docker-compose-full.yml down

# Stop and remove volumes
docker compose -f infra/docker-compose-full.yml down -v

# View logs
docker compose -f infra/docker-compose-full.yml logs -f

# Restart a specific service
docker compose -f infra/docker-compose-full.yml restart kafka
```

## Environment Files

### Local Development (`env.local`)

```bash
# Copy example
cp infra/env-example.local infra/env.local

# Source it
source infra/env.local
```

Contains:
- `KAFKA_BOOTSTRAP_SERVERS=localhost:9092`
- `SCHEMA_REGISTRY_URL=http://localhost:8081`
- `KAFKA_UI_URL=http://localhost:8080`
- Security: `PLAINTEXT` (no authentication)

### AWS MSK (`env.msk`)

```bash
# Copy example
cp infra/env-example.msk infra/env.msk

# Edit with your MSK details
vim infra/env.msk

# Source it
source infra/env.msk
```

Update:
- `KAFKA_BOOTSTRAP_SERVERS` - your MSK bootstrap servers
- Security settings as per your MSK configuration

## Data Persistence

Data is persisted in Docker volumes:
- `kafka-tutorials-zookeeper-data`
- `kafka-tutorials-zookeeper-logs`
- `kafka-tutorials-kafka-data`
- `kafka-tutorials-schema-registry-data`

To completely reset (delete all topics, messages, schemas):

```bash
make kafka-stop-all
```

## Troubleshooting

### Services won't start

Check if ports are already in use:

```bash
# Check port 9092 (Kafka)
lsof -i :9092

# Check port 8081 (Schema Registry)
lsof -i :8081

# Check port 8080 (Kafka UI)
lsof -i :8080
```

### Connection timeout

Wait 30-60 seconds after `make kafka-start` for all services to fully initialize:

```bash
# Check readiness
make kafka-status

# Watch logs
make kafka-logs
```

### Can't connect from tests

Make sure you've sourced the environment file:

```bash
source infra/env.local

# Verify it's set
echo $KAFKA_BOOTSTRAP_SERVERS
# Should print: localhost:9092
```

### Reset everything

```bash
# Stop and remove all data
make kafka-stop-all

# Remove dangling volumes
docker volume prune

# Start fresh
make kafka-start
```

## Schema Registry Usage

### Register a Schema (via REST API)

```bash
# Example: Register an Avro schema for a topic
curl -X POST http://localhost:8081/subjects/my-topic-value/versions \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{
    "schema": "{\"type\":\"record\",\"name\":\"User\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"age\",\"type\":\"int\"}]}"
  }'
```

### List All Subjects

```bash
curl http://localhost:8081/subjects
```

### Get Schema for Subject

```bash
curl http://localhost:8081/subjects/my-topic-value/versions/latest
```

Or use the **Kafka UI** at `http://localhost:8080` to manage schemas visually!

## Using Kafka UI

Open `http://localhost:8080` in your browser to:

- **Browse Topics**: View all topics, partitions, and messages
- **Produce Messages**: Send test messages directly from the UI
- **Consume Messages**: Read messages with filtering
- **View Schemas**: Browse and manage Avro/Protobuf schemas
- **Monitor Consumers**: Check consumer group lag
- **Cluster Info**: View broker health and configurations

Perfect for learning and debugging!

## EC2 Deployment with External Access

For deploying on EC2 with external client access using SASL_PLAINTEXT authentication, see:

**[EC2 Deployment Guide](README-EC2-DEPLOYMENT.md)**

The guide covers:
- Setting up Kafka with SASL_PLAINTEXT on EC2
- Auto-detecting EC2 public IP
- Configuring security groups
- Client connection examples (Java, Python, CLI)
- Troubleshooting tips

Quick summary:
- External access port: `19093` (SASL_PLAINTEXT)
- Default credentials: `admin/admin` (change in production!)
- Bootstrap servers: `<EC2_PUBLIC_IP>:19093`

