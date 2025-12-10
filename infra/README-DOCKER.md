# Docker Compose Configurations for Kafka

This directory contains multiple Docker Compose configurations for different use cases.

## Available Configurations

### 1. Apache Kafka (Recommended for Tutorials) ✅

**File:** `docker-compose-apache.yml`

Pure Apache Kafka using Bitnami images with KRaft mode (no Zookeeper needed).

```bash
# Start
make kafka-apache-start

# Or manually
docker compose -f infra/docker-compose-apache.yml up -d
```

**Features:**
- ✅ Pure Apache Kafka (not Confluent)
- ✅ KRaft mode (modern, no Zookeeper)
- ✅ Kafka UI included
- ✅ Single broker setup
- ✅ Auto-create topics enabled
- ✅ Optimized for local development

**Access:**
- Kafka: `localhost:9092` (internal) or `localhost:9094` (external)
- Kafka UI: http://localhost:8080

**Use for:**
- All advanced chapter tutorials
- Learning Kafka fundamentals
- Local development
- Testing scripts

---

### 2. Confluent Platform (Full Stack)

**File:** `docker-compose-full.yml`

Confluent Platform with Schema Registry and Kafka UI.

```bash
# Start
make kafka-start

# Or manually
docker compose -f infra/docker-compose-full.yml up -d
```

**Features:**
- Confluent Kafka
- Zookeeper
- Schema Registry
- Kafka UI

**Access:**
- Kafka: `localhost:9092`
- Schema Registry: http://localhost:8081
- Kafka UI: http://localhost:8080

**Use for:**
- Schema Registry testing
- Avro/Protobuf serialization
- Advanced serialization chapter

---

### 3. Simple Kafka Only

**File:** `docker-compose.kafka.yml`

Minimal Confluent Kafka with Zookeeper.

```bash
# Start
make kafka-start-simple

# Or manually
docker compose -f infra/docker-compose.kafka.yml up -d
```

**Features:**
- Confluent Kafka
- Zookeeper
- No additional services

**Use for:**
- Minimal Kafka testing
- Basic tutorials

---

## Quick Start Guide

### Option 1: Automated Setup (Easiest)

```bash
# Complete setup: start Kafka, create env file, test connection
make setup-and-test

# Test Advanced Chapter 01
make test-adv-ch01
```

### Option 2: Manual Setup

```bash
# 1. Start Apache Kafka
make kafka-apache-start

# 2. Create environment file
cp infra/env-example.local infra/env.local

# 3. Update env.local if needed (default is localhost:9092)
# Edit infra/env.local

# 4. Source environment
source infra/env.local

# 5. Test connection
make test-connection

# 6. Run advanced chapter tests
make test-adv-ch01  # Chapter 01: Reliability
make test-adv-ch02  # Chapter 02: Performance
# ... etc
```

---

## Common Commands

### Starting Kafka

```bash
# Apache Kafka (recommended)
make kafka-apache-start

# Confluent Platform with Schema Registry
make kafka-start

# Simple Confluent Kafka only
make kafka-start-simple
```

### Stopping Kafka

```bash
# Apache Kafka (keep data)
make kafka-apache-stop

# Apache Kafka (remove all data)
make kafka-apache-clean

# Confluent (keep data)
make kafka-stop

# Confluent (remove all data)
make kafka-stop-all
```

### Monitoring

```bash
# Check status
make kafka-apache-status

# View logs
make kafka-apache-logs

# Or for Confluent
make kafka-status
make kafka-logs
```

---

## Testing Advanced Chapters

All advanced chapters have dedicated test targets:

```bash
# Individual chapters
make test-adv-ch01  # Reliability (3-5 min)
make test-adv-ch02  # Performance (5-10 min)
make test-adv-ch03  # Partitioning (3-5 min)
make test-adv-ch04  # Serialization (3-5 min)
make test-adv-ch05  # Error Handling (3-5 min)
make test-adv-ch06  # Operations (2-3 min)

# All chapters (25-40 min total)
make test-adv-all

# Quick test (1-2 min)
make test-quick
```

---

## Kafka UI

All configurations include Kafka UI for visualization:

**URL:** http://localhost:8080

**Features:**
- Browse topics and messages
- View consumer groups
- Monitor cluster health
- Create topics
- Publish messages
- View configurations

---

## Troubleshooting

### Port Already in Use

If you get "port already in use" errors:

```bash
# Check what's using the port
lsof -i :9092
lsof -i :8080

# Stop any running Kafka containers
docker ps | grep kafka
docker stop <container-id>

# Or stop all
make kafka-apache-clean
make kafka-stop-all
```

### Kafka Not Ready

If tests fail with connection errors:

```bash
# Check container status
make kafka-apache-status

# View logs
make kafka-apache-logs

# Restart if needed
make kafka-apache-stop
make kafka-apache-start

# Wait longer for startup (containers need 15-20 seconds)
sleep 20
make test-connection
```

### Clean Start

For a completely fresh start:

```bash
# Stop and remove all Kafka data
make kafka-apache-clean

# Start fresh
make kafka-apache-start

# Wait for ready
sleep 20

# Test
make test-connection
```

---

## Environment Variables

The tutorials use environment variables for configuration:

**File:** `infra/env.local` (copy from `env-example.local`)

```bash
# Kafka connection
KAFKA_BOOTSTRAP_SERVERS="localhost:9092"

# Security (for local development)
KAFKA_SECURITY_PROTOCOL="PLAINTEXT"
KAFKA_SASL_MECHANISM=""
KAFKA_SASL_USERNAME=""
KAFKA_SASL_PASSWORD=""

# Additional config
KAFKA_ADDITIONAL_CONFIG=""
```

**Usage:**
```bash
# Source before running tests
source infra/env.local

# Or inline
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 bash/chapters/...
```

---

## Performance Tuning

The Apache Kafka configuration includes optimizations for local development:

- **Auto-create topics:** Enabled
- **Replication factor:** 1 (single broker)
- **Min ISR:** 1
- **Message size:** Up to 10MB (for testing)
- **Retention:** 7 days

For production settings, see Chapter 06: Operational Concerns.

---

## Next Steps

1. **Start Kafka:** `make setup-and-test`
2. **Open Kafka UI:** http://localhost:8080
3. **Run Chapter 01:** `make test-adv-ch01`
4. **Continue Learning:** Work through chapters 01-06

Happy Kafka learning! 🚀
