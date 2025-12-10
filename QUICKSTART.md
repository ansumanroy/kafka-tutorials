# Quick Start Guide - Kafka Tutorials with Docker

This guide will get you up and running with Kafka and the advanced chapter tutorials in under 2 minutes.

## Prerequisites

- Docker Desktop installed and running
- `make` command available (comes with macOS/Linux)
- Terminal/shell access

## One-Command Setup

```bash
make setup-and-test
```

This single command will:
1. ✅ Start Apache Kafka cluster (Zookeeper + Kafka + Kafka UI)
2. ✅ Wait for services to be ready
3. ✅ Create environment configuration
4. ✅ Test Kafka connection
5. ✅ Confirm you're ready to run tutorials

**That's it!** You're ready to go. 🚀

---

## Quick Test - Run Advanced Chapter 01

```bash
make test-adv-ch01
```

This will run the reliability and delivery guarantees tests (~2-3 minutes).

---

## What Just Happened?

### Services Started

- **Apache Kafka**: Pure Apache Kafka (wurstmeister image)
  - Access: `localhost:9092`
  - Version: 2.8.1
  
- **Zookeeper**: Coordination service
  - Access: `localhost:2181`

- **Kafka UI**: Web interface for visualization
  - Access: http://localhost:8080
  - Browse topics, messages, consumer groups, etc.

### Kafka CLI Tools via Docker

The setup automatically provides Kafka CLI commands that run via Docker, so you **don't need to install Kafka locally**.

Commands available:
- `kafka-topics.sh`
- `kafka-console-producer.sh`
- `kafka-console-consumer.sh`
- `kafka-consumer-groups.sh`
- `kafka-producer-perf-test.sh`
- `kafka-configs.sh`
- `kafka-log-dirs.sh`

These are wrappers that execute commands inside the Kafka container.

---

## Running All Advanced Chapters

### Individual Chapters (3-5 minutes each)

```bash
make test-adv-ch01  # Reliability & Delivery Guarantees
make test-adv-ch02  # Performance & Throughput
make test-adv-ch03  # Keys, Partitioning, and Ordering
make test-adv-ch04  # Serialization & Schema Management
make test-adv-ch05  # Error Handling & Observability
make test-adv-ch06  # Operational Concerns
```

### All Chapters (25-40 minutes total)

```bash
make test-adv-all
```

### Quick Test (1-2 minutes)

```bash
make test-quick  # Just runs Chapter 01 with 10 messages
```

---

## Managing Kafka

### Check Status

```bash
make kafka-apache-status
```

### View Logs

```bash
make kafka-apache-logs
```

### Stop Kafka (keeps data)

```bash
make kafka-apache-stop
```

### Stop and Remove All Data

```bash
make kafka-apache-clean
```

### Restart

```bash
make kafka-apache-stop
make kafka-apache-start
```

---

## Kafka UI

Open http://localhost:8080 in your browser to:

- 📊 Browse topics and messages
- 👥 View consumer groups and lag
- ⚙️ Check cluster configuration
- 📝 Create topics
- 🔍 Search and filter messages
- 📈 Monitor broker health

**No login required** - it's pre-configured for localhost.

---

## Environment Configuration

The setup automatically creates `infra/env.local`:

```bash
# View configuration
cat infra/env.local

# Source it manually if needed
source infra/env.local

# Use Kafka CLI tools
kafka-topics.sh --bootstrap-server localhost:9092 --list
```

---

## Manual Testing

If you want to test manually:

```bash
# Source environment
source infra/env.local

# List topics
kafka-topics.sh --bootstrap-server localhost:9092 --list

# Create a topic
kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic my-test-topic --partitions 3 --replication-factor 1

# Produce messages
echo "hello kafka" | kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic my-test-topic

# Consume messages
kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic my-test-topic --from-beginning --max-messages 1
```

---

## Advanced Chapter Details

### Chapter 01: Reliability & Delivery Guarantees
**What it tests:**
- Different `acks` configurations (1, all)
- Idempotence and exactly-once semantics
- Retry behavior with backoff
- Timeout scenarios
- Failure simulation

**Duration:** 2-3 minutes

---

### Chapter 02: Performance & Throughput
**What it tests:**
- Compression algorithms (none, gzip, snappy, lz4)
- Batch size optimization
- Linger time effects
- Throughput benchmarks

**Duration:** 5-10 minutes

---

### Chapter 03: Keys, Partitioning, and Ordering
**What it tests:**
- Key-based partitioning
- Partition distribution
- Hot partition problems
- Ordering guarantees

**Duration:** 3-5 minutes

---

### Chapter 04: Serialization & Schema Management
**What it tests:**
- String vs JSON serialization
- Message size comparison
- Schema evolution concepts
- Format compatibility

**Duration:** 3-5 minutes

---

### Chapter 05: Error Handling & Observability
**What it tests:**
- Error scenarios and handling
- Dead Letter Queue (DLQ) pattern
- Metrics and monitoring
- Health checks

**Duration:** 3-5 minutes

---

### Chapter 06: Operational Concerns
**What it tests:**
- Graceful shutdown patterns
- Security configurations
- Deployment checklist
- Production best practices

**Duration:** 2-3 minutes

---

## Troubleshooting

### Port Already in Use

```bash
# Check what's using port 9092
lsof -i :9092

# Stop all Kafka containers
make kafka-apache-clean
docker ps | grep kafka
```

### Kafka Not Responding

```bash
# Check container status
make kafka-apache-status

# View logs for errors
make kafka-apache-logs

# Restart fresh
make kafka-apache-clean
make kafka-apache-start
sleep 15
make test-connection
```

### Permission Denied for Docker

Make sure Docker Desktop is running:
```bash
# Check Docker is running
docker ps

# If not, start Docker Desktop application
```

### Scripts Can't Find Kafka Tools

The scripts use Docker-based wrappers. Make sure:
```bash
# Check env.local exists
ls -la infra/env.local

# Source it
source infra/env.local

# Verify wrappers exist
ls -la bin/kafka-*.sh

# Test a command
kafka-topics.sh --version
```

---

## Complete Workflow Example

Here's a complete workflow from scratch:

```bash
# 1. Setup everything
make setup-and-test

# 2. Open Kafka UI
open http://localhost:8080

# 3. Run Chapter 01
make test-adv-ch01

# 4. Run Chapter 02 (performance)
make test-adv-ch02

# 5. Check topics in UI (refresh browser)

# 6. Run remaining chapters
make test-adv-ch03
make test-adv-ch04
make test-adv-ch05
make test-adv-ch06

# 7. When done, clean up
make kafka-apache-clean
```

---

## Help

View all available commands:

```bash
make help
```

This shows:
- 🚀 Quick Start commands
- 📦 Apache Kafka management
- 🧪 Advanced chapter testing
- 🐍 Python targets
- ☕ Java targets
- 💡 Tips and examples

---

## Next Steps

1. ✅ **Start with Quick Start**: Run `make setup-and-test`
2. 📚 **Learn the Basics**: Read `bash/chapters/adv_chapter_01_reliability/README.md`
3. 🧪 **Run Tests**: Execute `make test-adv-ch01` through `make test-adv-ch06`
4. 📊 **Explore Kafka UI**: Open http://localhost:8080
5. 💻 **Try Manual Commands**: Source `infra/env.local` and experiment
6. 🔍 **Deep Dive**: Read the detailed READMEs in each chapter folder

---

## Additional Resources

- **Docker Compose Details**: See `infra/README-DOCKER.md`
- **Advanced Producers Guide**: See `Advanced_producers.md`
- **Java Examples**: See `java/README.md`
- **Python Examples**: See `python/README.md`

---

## Summary of Key Commands

| Command | Description | Time |
|---------|-------------|------|
| `make setup-and-test` | Complete setup + test | 1-2 min |
| `make test-quick` | Quick validation | 1-2 min |
| `make test-adv-ch01` | Test Chapter 01 | 2-3 min |
| `make test-adv-all` | Test all chapters | 25-40 min |
| `make kafka-apache-status` | Check Kafka status | instant |
| `make kafka-apache-logs` | View Kafka logs | ongoing |
| `make kafka-apache-clean` | Stop and clean | instant |
| `make help` | Show all commands | instant |

---

Happy Kafka learning! 🚀

For issues or questions, check:
- Terminal output for detailed error messages
- Kafka logs: `make kafka-apache-logs`
- Kafka UI: http://localhost:8080
- Container status: `make kafka-apache-status`
