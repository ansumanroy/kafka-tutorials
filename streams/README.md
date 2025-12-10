# Kafka Streams Tutorials

Comprehensive Java-based tutorials for Apache Kafka Streams, from basics to production.

## 🚀 Quick Start

```bash
# Start Kafka
cd /Users/ansumanroy/projects/kafka-tutorials
make kafka-apache-start

# Source environment
source infra/env.local

# Run a streams example
cd streams/chapter_01_introduction
gradle build
gradle runWordCount
```

## 📚 Chapters

### Basic Chapters (01-03)

| Chapter | Topic | Key Concepts |
|---------|-------|--------------|
| **01** | [Introduction](chapter_01_introduction/) | Streams API, Topology, KStream vs KTable |
| **02** | [KStream Basics](chapter_02_kstream_basics/) | filter, map, flatMap, branch |
| **03** | [KTable](chapter_03_ktable/) | Stateful processing, GlobalKTable, changelog |

### Advanced Chapters (04-07)

| Chapter | Topic | Key Concepts |
|---------|-------|--------------|
| **04** | [Joins](chapter_04_joins/) | Stream-stream, stream-table, table-table |
| **05** | [Windowing](chapter_05_windowing/) | Tumbling, hopping, session windows |
| **06** | [Aggregations](chapter_06_aggregations/) | count, reduce, aggregate, materialized views |
| **07** | [State Stores](chapter_07_state_stores/) | KeyValueStore, persistence, interactive queries |

### Production Chapters (08-09)

| Chapter | Topic | Key Concepts |
|---------|-------|--------------|
| **08** | [Topology Design](chapter_08_topology/) | Sub-topologies, testing, optimization |
| **09** | [Exactly-Once](chapter_09_exactly_once/) | EOS v2, transactions, configuration |

### Integration (10)

| Chapter | Topic | Key Concepts |
|---------|-------|--------------|
| **10** | [KSQL/ksqlDB](chapter_10_ksqldb/) | SQL interface, Docker setup, Java client |

## 🎯 Learning Path

**Beginners** (Start here):
1. Chapter 01: Introduction → Understand core concepts
2. Chapter 02: KStream → Learn stateless operations
3. Chapter 03: KTable → Learn stateful basics

**Intermediate**:
4. Chapter 04: Joins → Combine streams and tables
5. Chapter 05: Windowing → Time-based processing
6. Chapter 06: Aggregations → Build analytics

**Advanced**:
7. Chapter 07: State Stores → Deep dive into state
8. Chapter 08: Topology → Design best practices
9. Chapter 09: Exactly-Once → Production guarantees

**Alternative Approach**:
10. Chapter 10: KSQL → SQL-based stream processing

## 🛠️ Prerequisites

- Java 17+
- Gradle
- Running Kafka cluster (local or MSK)
- Docker (for KSQL chapter)

## 📖 Documentation Structure

Each chapter includes:
- **README.md**: Concepts, examples, best practices
- **Java code**: Working implementations
- **Tests**: Unit tests with TopologyTestDriver
- **Gradle tasks**: Easy execution

## 🧪 Running Tests

```bash
# From chapter directory
gradle test

# Or from root via Makefile
make streams-ch01-test
make streams-ch02-test
# ... etc
```

## 🎓 Key Takeaways by Chapter

### Chapter 01: Introduction
- Streams API is a library, not a framework
- Topology = DAG of processors
- KStream = events, KTable = state

### Chapter 02: KStream
- Stateless operations process records independently
- mapValues() avoids repartitioning
- Filter early for performance

### Chapter 03: KTable
- KTable maintains latest value per key
- GlobalKTable = fully replicated (no co-partitioning)
- Changelog topics provide fault tolerance

### Chapter 04: Joins
- Stream-stream requires windowing
- Stream-table for enrichment
- Co-partitioning required (except GlobalKTable)

### Chapter 05: Windowing
- Tumbling = fixed, non-overlapping
- Hopping = fixed, overlapping
- Session = variable, activity-based

### Chapter 06: Aggregations
- count() = simplest aggregation
- reduce() = combine same-type values
- aggregate() = most flexible

### Chapter 07: State Stores
- RocksDB backing for persistence
- Changelog for replication
- Interactive queries for external access

### Chapter 08: Topology
- Minimize repartitioning
- Name operations for debugging
- TopologyTestDriver for fast tests

### Chapter 09: Exactly-Once
- EOS v2 recommended
- Atomic read-process-write
- Trade-off: correctness vs performance

### Chapter 10: KSQL
- SQL interface for streams
- Push queries = streaming
- Pull queries = point-in-time

## 🎬 Example Workflows

### Word Count (Classic)
```bash
cd chapter_01_introduction
gradle runWordCount
```

### Real-time Analytics
```bash
cd chapter_06_aggregations
gradle runCount
```

### Stream Enrichment
```bash
cd chapter_04_joins
gradle runTableJoin
```

## 🐳 Docker Setup

For KSQL chapter:
```bash
docker-compose -f chapter_10_ksqldb/docker-compose-ksqldb.yml up -d
```

## 📊 Slides

See [STREAMS_SLIDES.md](STREAMS_SLIDES.md) for a comprehensive presentation covering all chapters.

## 🤝 Contributing

When adding new chapters:
1. Follow existing structure
2. Include tests with TopologyTestDriver
3. Add comprehensive README
4. Update this main README

## 🔗 Resources

- [Kafka Streams Documentation](https://kafka.apache.org/documentation/streams/)
- [Kafka Streams Examples](https://github.com/apache/kafka/tree/trunk/streams/examples)
- [Confluent Tutorials](https://kafka-tutorials.confluent.io/)
- [ksqlDB Documentation](https://docs.ksqldb.io/)

---

**Happy Streaming! 🚀**
