PYTHON_ENV ?= .venv
PYTHON ?= python3
JAVA_ADV_RELIABILITY_DIR = java/adv_chapter_01_reliability
JAVA_ADV_PERFORMANCE_DIR = java/adv_chapter_02_performance
JAVA_ADV_PARTITIONING_DIR = java/adv_chapter_03_partitioning
JAVA_ADV_SERIALIZATION_DIR = java/adv_chapter_04_serialization
JAVA_ADV_ERROR_HANDLING_DIR = java/adv_chapter_05_error_handling
JAVA_ADV_CIRCUIT_BREAKER_DIR = java/adv_chapter_07_circuit_breaker

# Kafka Streams chapter directories
STREAMS_CH01_DIR = streams/chapter_01_introduction
STREAMS_CH02_DIR = streams/chapter_02_kstream_basics
STREAMS_CH03_DIR = streams/chapter_03_ktable
STREAMS_CH04_DIR = streams/chapter_04_joins
STREAMS_CH05_DIR = streams/chapter_05_windowing
STREAMS_CH06_DIR = streams/chapter_06_aggregations
STREAMS_CH07_DIR = streams/chapter_07_state_stores
STREAMS_CH08_DIR = streams/chapter_08_topology
STREAMS_CH09_DIR = streams/chapter_09_exactly_once
STREAMS_CH10_DIR = streams/chapter_10_ksqldb

# Default target when running 'make' without arguments
.DEFAULT_GOAL := help

# ==========================================
# Python Targets
# ==========================================

.PHONY: python-env
python-env:
	$(PYTHON) -m venv $(PYTHON_ENV)
	$(PYTHON_ENV)/bin/pip install --upgrade pip
	$(PYTHON_ENV)/bin/pip install -r python/requirements.txt

.PHONY: python-check-connection
python-check-connection: python-env
	. infra/env.msk 2>/dev/null || . infra/env.local 2>/dev/null || true
	$(PYTHON_ENV)/bin/python python/ch01_environment/check_connection.py

# ==========================================
# Java Targets
# ==========================================

# Chapter 01: Reliability
.PHONY: java-reliability-build
java-reliability-build:
	@echo "Building Java reliability tests..."
	cd $(JAVA_ADV_RELIABILITY_DIR) && gradle build -x test

.PHONY: java-reliability-test
java-reliability-test:
	@echo "Running all Java reliability tests..."
	@echo "Make sure to source infra/env.msk or infra/env.local first!"
	cd $(JAVA_ADV_RELIABILITY_DIR) && gradle test

.PHONY: java-reliability-integration
java-reliability-integration:
	@echo "Running Java reliability integration tests..."
	@echo "Make sure to source infra/env.msk or infra/env.local first!"
	cd $(JAVA_ADV_RELIABILITY_DIR) && gradle runReliabilityTests

.PHONY: java-reliability-failure
java-reliability-failure:
	@echo "Running Java failure simulation tests..."
	@echo "Make sure to source infra/env.msk or infra/env.local first!"
	cd $(JAVA_ADV_RELIABILITY_DIR) && gradle runFailureTests

.PHONY: java-reliability-clean
java-reliability-clean:
	@echo "Cleaning Java reliability build artifacts..."
	cd $(JAVA_ADV_RELIABILITY_DIR) && gradle clean

# Chapter 02: Performance
JAVA_ADV_PERFORMANCE_DIR = java/adv_chapter_02_performance

.PHONY: java-performance-build
java-performance-build:
	@echo "Building Java performance tests..."
	cd $(JAVA_ADV_PERFORMANCE_DIR) && gradle build -x test

.PHONY: java-performance-test
java-performance-test:
	@echo "Running all Java performance tests..."
	@echo "Make sure to source infra/env.local first!"
	@if [ ! -f infra/env.local ]; then \
		echo "Creating env.local..."; \
		cp infra/env-example.local infra/env.local; \
	fi
	@bash -c 'source infra/env.local && cd $(JAVA_ADV_PERFORMANCE_DIR) && gradle test'

.PHONY: java-performance-benchmark
java-performance-benchmark:
	@echo "Running Java performance benchmark..."
	@echo "Make sure to source infra/env.local first!"
	@if [ ! -f infra/env.local ]; then \
		echo "Creating env.local..."; \
		cp infra/env-example.local infra/env.local; \
	fi
	@bash -c 'source infra/env.local && cd $(JAVA_ADV_PERFORMANCE_DIR) && gradle runBenchmark'

.PHONY: java-performance-tests-only
java-performance-tests-only:
	@echo "Running Java performance benchmark tests..."
	@bash -c 'source infra/env.local && cd $(JAVA_ADV_PERFORMANCE_DIR) && gradle runPerformanceTests'

.PHONY: java-performance-compression
java-performance-compression:
	@echo "Running Java compression comparison tests..."
	@bash -c 'source infra/env.local && cd $(JAVA_ADV_PERFORMANCE_DIR) && gradle runCompressionTests'

.PHONY: java-performance-clean
java-performance-clean:
	@echo "Cleaning Java performance build artifacts..."
	cd $(JAVA_ADV_PERFORMANCE_DIR) && gradle clean

# Chapter 03: Partitioning
.PHONY: java-partitioning-build
java-partitioning-build:
	@echo "Building Java partitioning tests..."
	cd $(JAVA_ADV_PARTITIONING_DIR) && gradle build -x test

.PHONY: java-partitioning-test
java-partitioning-test:
	@echo "Running all Java partitioning tests..."
	@echo "Make sure to source infra/env.local first!"
	@if [ ! -f infra/env.local ]; then \
		echo "Creating env.local..."; \
		cp infra/env-example.local infra/env.local; \
	fi
	@bash -c 'source infra/env.local && cd $(JAVA_ADV_PARTITIONING_DIR) && gradle test'

.PHONY: java-partitioning-demo
java-partitioning-demo:
	@echo "Running Java partitioning demonstration..."
	@bash -c 'source infra/env.local && cd $(JAVA_ADV_PARTITIONING_DIR) && gradle runPartitioningDemo'

.PHONY: java-partitioning-behavior
java-partitioning-behavior:
	@echo "Running Java partitioning behavior tests..."
	@bash -c 'source infra/env.local && cd $(JAVA_ADV_PARTITIONING_DIR) && gradle runPartitioningTests'

.PHONY: java-partitioning-hotpartition
java-partitioning-hotpartition:
	@echo "Running Java hot partition tests..."
	@bash -c 'source infra/env.local && cd $(JAVA_ADV_PARTITIONING_DIR) && gradle runHotPartitionTests'

.PHONY: java-partitioning-clean
java-partitioning-clean:
	@echo "Cleaning Java partitioning build artifacts..."
	cd $(JAVA_ADV_PARTITIONING_DIR) && gradle clean

# Chapter 04: Serialization
.PHONY: java-serialization-build
java-serialization-build:
	@echo "Building Java serialization tests (includes Avro code generation)..."
	cd $(JAVA_ADV_SERIALIZATION_DIR) && gradle build -x test

.PHONY: java-serialization-test
java-serialization-test:
	@echo "Running all Java serialization tests..."
	@echo "Make sure Schema Registry is running (make kafka-start)!"
	@if [ ! -f infra/env.local ]; then \
		echo "Creating env.local..."; \
		cp infra/env-example.local infra/env.local; \
	fi
	@bash -c 'source infra/env.local && cd $(JAVA_ADV_SERIALIZATION_DIR) && gradle test'

.PHONY: java-serialization-demo
java-serialization-demo:
	@echo "Running Java serialization demonstration..."
	@bash -c 'source infra/env.local && cd $(JAVA_ADV_SERIALIZATION_DIR) && gradle runSerializationDemo'

.PHONY: java-serialization-formats
java-serialization-formats:
	@echo "Running Java format comparison tests..."
	@bash -c 'source infra/env.local && cd $(JAVA_ADV_SERIALIZATION_DIR) && gradle runFormatTests'

.PHONY: java-serialization-schema
java-serialization-schema:
	@echo "Running Java schema evolution tests..."
	@bash -c 'source infra/env.local && cd $(JAVA_ADV_SERIALIZATION_DIR) && gradle runSchemaTests'

.PHONY: java-serialization-avro
java-serialization-avro:
	@echo "Running Java Avro serialization tests..."
	@bash -c 'source infra/env.local && cd $(JAVA_ADV_SERIALIZATION_DIR) && gradle runAvroTests'

.PHONY: java-serialization-clean
java-serialization-clean:
	@echo "Cleaning Java serialization build artifacts..."
	cd $(JAVA_ADV_SERIALIZATION_DIR) && gradle clean

# Chapter 05: Error Handling & Observability
.PHONY: java-errorhandling-build
java-errorhandling-build:
	@echo "Building Java error handling tests..."
	cd $(JAVA_ADV_ERROR_HANDLING_DIR) && gradle build -x test

.PHONY: java-errorhandling-test
java-errorhandling-test:
	@echo "Running all Java error handling tests..."
	@if [ ! -f infra/env.local ]; then \
		echo "Creating env.local..."; \
		cp infra/env-example.local infra/env.local; \
	fi
	@bash -c 'source infra/env.local && cd $(JAVA_ADV_ERROR_HANDLING_DIR) && gradle test'

.PHONY: java-errorhandling-demo
java-errorhandling-demo:
	@echo "Running Java error handling demonstration..."
	@bash -c 'source infra/env.local && cd $(JAVA_ADV_ERROR_HANDLING_DIR) && gradle runErrorDemo'

.PHONY: java-errorhandling-dlq
java-errorhandling-dlq:
	@echo "Running Java Dead Letter Queue demonstration..."
	@bash -c 'source infra/env.local && cd $(JAVA_ADV_ERROR_HANDLING_DIR) && gradle runDlqDemo'

.PHONY: java-errorhandling-metrics
java-errorhandling-metrics:
	@echo "Running Java metrics demonstration..."
	@bash -c 'source infra/env.local && cd $(JAVA_ADV_ERROR_HANDLING_DIR) && gradle runMetricsDemo'

.PHONY: java-errorhandling-error-tests
java-errorhandling-error-tests:
	@echo "Running Java error handling tests..."
	@bash -c 'source infra/env.local && cd $(JAVA_ADV_ERROR_HANDLING_DIR) && gradle runErrorTests'

.PHONY: java-errorhandling-dlq-tests
java-errorhandling-dlq-tests:
	@echo "Running Java DLQ tests..."
	@bash -c 'source infra/env.local && cd $(JAVA_ADV_ERROR_HANDLING_DIR) && gradle runDlqTests'

.PHONY: java-errorhandling-metrics-tests
java-errorhandling-metrics-tests:
	@echo "Running Java metrics tests..."
	@bash -c 'source infra/env.local && cd $(JAVA_ADV_ERROR_HANDLING_DIR) && gradle runMetricsTests'

.PHONY: java-errorhandling-clean
java-errorhandling-clean:
	@echo "Cleaning Java error handling build artifacts..."
	cd $(JAVA_ADV_ERROR_HANDLING_DIR) && gradle clean

# Chapter 07: Circuit Breaker Pattern
.PHONY: java-circuitbreaker-build
java-circuitbreaker-build:
	@echo "Building Java circuit breaker tests..."
	cd $(JAVA_ADV_CIRCUIT_BREAKER_DIR) && gradle build -x test

.PHONY: java-circuitbreaker-test
java-circuitbreaker-test:
	@echo "Running all Java circuit breaker tests..."
	@if [ ! -f infra/env.local ]; then \
		echo "Creating env.local..."; \
		cp infra/env-example.local infra/env.local; \
	fi
	@bash -c 'source infra/env.local && cd $(JAVA_ADV_CIRCUIT_BREAKER_DIR) && gradle test'

.PHONY: java-circuitbreaker-demo
java-circuitbreaker-demo:
	@echo "Running Java circuit breaker demonstration..."
	@bash -c 'source infra/env.local && cd $(JAVA_ADV_CIRCUIT_BREAKER_DIR) && gradle runCircuitBreakerDemo'

.PHONY: java-circuitbreaker-clean
java-circuitbreaker-clean:
	@echo "Cleaning Java circuit breaker build artifacts..."
	cd $(JAVA_ADV_CIRCUIT_BREAKER_DIR) && gradle clean

# Bash Chapter 07: Circuit Breaker
.PHONY: test-adv-ch07
test-adv-ch07:
	@echo "Testing Advanced Chapter 07: Circuit Breaker Pattern"
	@bash -c 'source infra/env.local && bash bash/chapters/adv_chapter_07_circuit_breaker/demo_circuit_breaker.sh'

# ==========================================
# Kafka Cluster Management (Docker)
# ==========================================

.PHONY: kafka-start
kafka-start:
	@echo "Starting Kafka cluster (with Schema Registry and Kafka UI)..."
	docker compose -f infra/docker-compose-full.yml up -d
	@echo ""
	@echo "Waiting for services to be ready..."
	@sleep 5
	@echo ""
	@echo "✓ Kafka cluster started!"
	@echo ""
	@echo "Services available at:"
	@echo "  - Kafka:           localhost:9092"
	@echo "  - Schema Registry: http://localhost:8081"
	@echo "  - Kafka UI:        http://localhost:8080"
	@echo ""
	@echo "Next steps:"
	@echo "  1. Copy env file:      cp infra/env-example.local infra/env.local"
	@echo "  2. Source it:          source infra/env.local"
	@echo "  3. Verify connection:  bash/chapters/01-environment/check_connection.sh"

.PHONY: kafka-start-simple
kafka-start-simple:
	@echo "Starting simple Kafka cluster (without Schema Registry)..."
	docker compose -f infra/docker-compose.kafka.yml up -d
	@echo ""
	@echo "✓ Simple Kafka cluster started at localhost:9092"

.PHONY: kafka-stop
kafka-stop:
	@echo "Stopping Kafka cluster..."
	docker compose -f infra/docker-compose-full.yml down
	@echo "✓ Kafka cluster stopped"

.PHONY: kafka-stop-all
kafka-stop-all:
	@echo "Stopping all Kafka clusters and removing volumes..."
	docker compose -f infra/docker-compose-full.yml down -v
	docker compose -f infra/docker-compose.kafka.yml down -v 2>/dev/null || true
	@echo "✓ All Kafka clusters stopped and data removed"

.PHONY: kafka-status
kafka-status:
	@echo "Kafka Cluster Status:"
	@echo ""
	@docker compose -f infra/docker-compose-full.yml ps

.PHONY: kafka-logs
kafka-logs:
	@docker compose -f infra/docker-compose-full.yml logs -f

.PHONY: kafka-logs-kafka
kafka-logs-kafka:
	@docker compose -f infra/docker-compose-full.yml logs -f kafka

.PHONY: kafka-logs-schema-registry
kafka-logs-schema-registry:
	@docker compose -f infra/docker-compose-full.yml logs -f schema-registry

.PHONY: kafka-restart
kafka-restart: kafka-stop kafka-start

# ==========================================
# Kafka Streams Targets
# ==========================================

# Chapter 01: Introduction
.PHONY: streams-ch01-build streams-ch01-test streams-ch01-demo streams-ch01-clean
streams-ch01-build:
	@echo "Building Streams Chapter 01..."
	cd $(STREAMS_CH01_DIR) && gradle build -x test

streams-ch01-test:
	@echo "Testing Streams Chapter 01..."
	cd $(STREAMS_CH01_DIR) && gradle test

streams-ch01-demo:
	@echo "Running Streams Chapter 01 demo..."
	cd $(STREAMS_CH01_DIR) && gradle runWordCount

streams-ch01-clean:
	cd $(STREAMS_CH01_DIR) && gradle clean

# Chapter 02: KStream Basics
.PHONY: streams-ch02-build streams-ch02-test streams-ch02-filter streams-ch02-branch streams-ch02-clean
streams-ch02-build:
	@echo "Building Streams Chapter 02..."
	cd $(STREAMS_CH02_DIR) && gradle build -x test

streams-ch02-test:
	@echo "Testing Streams Chapter 02..."
	cd $(STREAMS_CH02_DIR) && gradle test

streams-ch02-filter:
	@echo "Running filter/map demo..."
	cd $(STREAMS_CH02_DIR) && gradle runFilterMap

streams-ch02-branch:
	@echo "Running branching demo..."
	cd $(STREAMS_CH02_DIR) && gradle runBranching

streams-ch02-clean:
	cd $(STREAMS_CH02_DIR) && gradle clean

# Chapter 03: KTable
.PHONY: streams-ch03-build streams-ch03-test streams-ch03-demo streams-ch03-global streams-ch03-clean
streams-ch03-build:
	@echo "Building Streams Chapter 03..."
	cd $(STREAMS_CH03_DIR) && gradle build -x test

streams-ch03-test:
	@echo "Testing Streams Chapter 03..."
	cd $(STREAMS_CH03_DIR) && gradle test

streams-ch03-demo:
	@echo "Running KTable demo..."
	cd $(STREAMS_CH03_DIR) && gradle runKTable

streams-ch03-global:
	@echo "Running GlobalKTable demo..."
	cd $(STREAMS_CH03_DIR) && gradle runGlobalKTable

streams-ch03-clean:
	cd $(STREAMS_CH03_DIR) && gradle clean

# Chapter 04: Joins
.PHONY: streams-ch04-build streams-ch04-test streams-ch04-stream streams-ch04-table streams-ch04-clean
streams-ch04-build:
	@echo "Building Streams Chapter 04..."
	cd $(STREAMS_CH04_DIR) && gradle build -x test

streams-ch04-test:
	@echo "Testing Streams Chapter 04..."
	cd $(STREAMS_CH04_DIR) && gradle test

streams-ch04-stream:
	@echo "Running stream-stream join demo..."
	cd $(STREAMS_CH04_DIR) && gradle runStreamJoin

streams-ch04-table:
	@echo "Running stream-table join demo..."
	cd $(STREAMS_CH04_DIR) && gradle runTableJoin

streams-ch04-clean:
	cd $(STREAMS_CH04_DIR) && gradle clean

# Chapter 05: Windowing
.PHONY: streams-ch05-build streams-ch05-test streams-ch05-demo streams-ch05-clean
streams-ch05-build:
	@echo "Building Streams Chapter 05..."
	cd $(STREAMS_CH05_DIR) && gradle build -x test

streams-ch05-test:
	@echo "Testing Streams Chapter 05..."
	cd $(STREAMS_CH05_DIR) && gradle test

streams-ch05-demo:
	@echo "Running windowing demo..."
	cd $(STREAMS_CH05_DIR) && gradle run

streams-ch05-clean:
	cd $(STREAMS_CH05_DIR) && gradle clean

# Chapter 06: Aggregations
.PHONY: streams-ch06-build streams-ch06-test streams-ch06-demo streams-ch06-clean
streams-ch06-build:
	@echo "Building Streams Chapter 06..."
	cd $(STREAMS_CH06_DIR) && gradle build -x test

streams-ch06-test:
	@echo "Testing Streams Chapter 06..."
	cd $(STREAMS_CH06_DIR) && gradle test

streams-ch06-demo:
	@echo "Running aggregations demo..."
	cd $(STREAMS_CH06_DIR) && gradle run

streams-ch06-clean:
	cd $(STREAMS_CH06_DIR) && gradle clean

# Chapter 07: State Stores
.PHONY: streams-ch07-build streams-ch07-test streams-ch07-demo streams-ch07-clean
streams-ch07-build:
	@echo "Building Streams Chapter 07..."
	cd $(STREAMS_CH07_DIR) && gradle build -x test

streams-ch07-test:
	@echo "Testing Streams Chapter 07..."
	cd $(STREAMS_CH07_DIR) && gradle test

streams-ch07-demo:
	@echo "Running state stores demo..."
	cd $(STREAMS_CH07_DIR) && gradle run

streams-ch07-clean:
	cd $(STREAMS_CH07_DIR) && gradle clean

# Chapter 08: Topology
.PHONY: streams-ch08-build streams-ch08-test streams-ch08-demo streams-ch08-clean
streams-ch08-build:
	@echo "Building Streams Chapter 08..."
	cd $(STREAMS_CH08_DIR) && gradle build -x test

streams-ch08-test:
	@echo "Testing Streams Chapter 08..."
	cd $(STREAMS_CH08_DIR) && gradle test

streams-ch08-demo:
	@echo "Running topology demo..."
	cd $(STREAMS_CH08_DIR) && gradle run

streams-ch08-clean:
	cd $(STREAMS_CH08_DIR) && gradle clean

# Chapter 09: Exactly-Once
.PHONY: streams-ch09-build streams-ch09-test streams-ch09-demo streams-ch09-clean
streams-ch09-build:
	@echo "Building Streams Chapter 09..."
	cd $(STREAMS_CH09_DIR) && gradle build -x test

streams-ch09-test:
	@echo "Testing Streams Chapter 09..."
	cd $(STREAMS_CH09_DIR) && gradle test

streams-ch09-demo:
	@echo "Running EOS demo..."
	cd $(STREAMS_CH09_DIR) && gradle run

streams-ch09-clean:
	cd $(STREAMS_CH09_DIR) && gradle clean

# Chapter 10: ksqlDB
.PHONY: streams-ch10-build streams-ch10-test streams-ch10-demo streams-ch10-clean
.PHONY: ksqldb-start ksqldb-stop ksqldb-cli
streams-ch10-build:
	@echo "Building Streams Chapter 10..."
	cd $(STREAMS_CH10_DIR) && gradle build -x test

streams-ch10-test:
	@echo "Testing Streams Chapter 10..."
	cd $(STREAMS_CH10_DIR) && gradle test

streams-ch10-demo:
	@echo "Running ksqlDB client demo..."
	cd $(STREAMS_CH10_DIR) && gradle runKsqlClient

streams-ch10-clean:
	cd $(STREAMS_CH10_DIR) && gradle clean

ksqldb-start:
	@echo "Starting ksqlDB server and CLI..."
	docker-compose -f $(STREAMS_CH10_DIR)/docker-compose-ksqldb.yml up -d

ksqldb-stop:
	@echo "Stopping ksqlDB..."
	docker-compose -f $(STREAMS_CH10_DIR)/docker-compose-ksqldb.yml down

ksqldb-cli:
	@echo "Connecting to ksqlDB CLI..."
	docker exec -it kafka-tutorials-ksqldb-cli ksql http://ksqldb-server:8088

# Build all streams chapters
.PHONY: streams-build-all
streams-build-all:
	@echo "Building all Streams chapters..."
	@$(MAKE) streams-ch01-build
	@$(MAKE) streams-ch02-build
	@$(MAKE) streams-ch03-build
	@$(MAKE) streams-ch04-build
	@$(MAKE) streams-ch05-build
	@$(MAKE) streams-ch06-build
	@$(MAKE) streams-ch07-build
	@$(MAKE) streams-ch08-build
	@$(MAKE) streams-ch09-build
	@$(MAKE) streams-ch10-build

# Test all streams chapters
.PHONY: streams-test-all
streams-test-all:
	@echo "Testing all Streams chapters..."
	@$(MAKE) streams-ch01-test
	@$(MAKE) streams-ch02-test
	@$(MAKE) streams-ch03-test
	@$(MAKE) streams-ch04-test
	@$(MAKE) streams-ch05-test
	@$(MAKE) streams-ch06-test
	@$(MAKE) streams-ch07-test
	@$(MAKE) streams-ch08-test
	@$(MAKE) streams-ch09-test
	@$(MAKE) streams-ch10-test

# Clean all streams chapters
.PHONY: streams-clean-all
streams-clean-all:
	@echo "Cleaning all Streams chapters..."
	@$(MAKE) streams-ch01-clean
	@$(MAKE) streams-ch02-clean
	@$(MAKE) streams-ch03-clean
	@$(MAKE) streams-ch04-clean
	@$(MAKE) streams-ch05-clean
	@$(MAKE) streams-ch06-clean
	@$(MAKE) streams-ch07-clean
	@$(MAKE) streams-ch08-clean
	@$(MAKE) streams-ch09-clean
	@$(MAKE) streams-ch10-clean

# Apache Kafka targets (pure Apache, not Confluent)
.PHONY: kafka-apache-start
kafka-apache-start:
	@echo "Starting Apache Kafka cluster (KRaft mode, no Zookeeper)..."
	docker compose -f infra/docker-compose-apache.yml up -d
	@echo ""
	@echo "Waiting for Kafka to be ready..."
	@sleep 15
	@echo ""
	@echo "✓ Apache Kafka cluster started!"
	@echo ""
	@echo "Services available at:"
	@echo "  - Kafka:    localhost:9092 (internal) or localhost:9094 (external)"
	@echo "  - Kafka UI: http://localhost:8080"
	@echo ""
	@echo "Next steps:"
	@echo "  1. Copy env file:      cp infra/env-example.local infra/env.local"
	@echo "  2. Update env.local:   Change port to 9092 if needed"
	@echo "  3. Source it:          source infra/env.local"
	@echo "  4. Test connection:    make test-connection"

.PHONY: kafka-apache-stop
kafka-apache-stop:
	@echo "Stopping Apache Kafka cluster..."
	docker compose -f infra/docker-compose-apache.yml down
	@echo "✓ Apache Kafka cluster stopped"

.PHONY: kafka-apache-clean
kafka-apache-clean:
	@echo "Stopping Apache Kafka cluster and removing volumes..."
	docker compose -f infra/docker-compose-apache.yml down -v
	@echo "✓ Apache Kafka cluster stopped and data removed"

.PHONY: kafka-apache-logs
kafka-apache-logs:
	@docker compose -f infra/docker-compose-apache.yml logs -f kafka

.PHONY: kafka-apache-status
kafka-apache-status:
	@echo "Apache Kafka Cluster Status:"
	@echo ""
	@docker compose -f infra/docker-compose-apache.yml ps

# ==========================================
# Advanced Chapter Testing
# ==========================================

.PHONY: test-connection
test-connection:
	@echo "Testing Kafka connection..."
	@echo ""
	@echo "Checking if Kafka container is running..."
	@docker ps | grep kafka-tutorials-kafka > /dev/null && echo "✓ Kafka container is running" || (echo "✗ Kafka container not found. Run 'make kafka-apache-start' first" && exit 1)
	@echo ""
	@echo "Testing Kafka broker connection..."
	@docker exec kafka-tutorials-kafka kafka-broker-api-versions.sh --bootstrap-server localhost:9092 2>&1 | grep -q "id:" && echo "✓ Kafka broker is responding" || (echo "✗ Kafka broker not responding" && exit 1)
	@echo ""
	@echo "Listing topics..."
	@docker exec kafka-tutorials-kafka kafka-topics.sh --bootstrap-server localhost:9092 --list 2>&1
	@echo ""
	@echo "✓ Kafka connection test passed!"

.PHONY: test-adv-ch01
test-adv-ch01:
	@echo "Testing Advanced Chapter 01: Reliability & Delivery Guarantees"
	@echo "=================================================="
	@echo ""
	@if [ ! -f infra/env.local ]; then \
		echo "Creating env.local..."; \
		cp infra/env-example.local infra/env.local; \
	fi
	@echo "Test 1: Reliability Test Suite"
	@bash -c 'source infra/env.local && bash bash/chapters/adv_chapter_01_reliability/test_reliability.sh'
	@echo ""
	@echo "Test 2: Failure Simulation"
	@bash -c 'source infra/env.local && bash bash/chapters/adv_chapter_01_reliability/simulate_failure.sh'
	@echo ""
	@echo "✓ Advanced Chapter 01 tests complete!"

.PHONY: test-adv-ch02
test-adv-ch02:
	@echo "Testing Advanced Chapter 02: Performance & Throughput"
	@echo "=================================================="
	@echo ""
	@echo "Test 1: Throughput Benchmark"
	@bash bash/chapters/adv_chapter_02_performance/benchmark_throughput.sh
	@echo ""
	@echo "Test 2: Configuration Comparison"
	@bash bash/chapters/adv_chapter_02_performance/compare_configs.sh
	@echo ""
	@echo "✓ Advanced Chapter 02 tests complete!"

.PHONY: test-adv-ch03
test-adv-ch03:
	@echo "Testing Advanced Chapter 03: Keys, Partitioning, and Ordering"
	@echo "=================================================="
	@echo ""
	@echo "Test 1: Partitioning Demo"
	@bash bash/chapters/adv_chapter_03_partitioning/demo_partitioning.sh
	@echo ""
	@echo "Test 2: Hot Partition Test"
	@bash bash/chapters/adv_chapter_03_partitioning/test_hot_partition.sh
	@echo ""
	@echo "✓ Advanced Chapter 03 tests complete!"

.PHONY: test-adv-ch04
test-adv-ch04:
	@echo "Testing Advanced Chapter 04: Serialization & Schema Management"
	@echo "=================================================="
	@echo ""
	@echo "Test 1: Format Demonstration"
	@bash bash/chapters/adv_chapter_04_serialization/demo_formats.sh
	@echo ""
	@echo "Test 2: Schema Evolution"
	@bash bash/chapters/adv_chapter_04_serialization/test_schema_evolution.sh
	@echo ""
	@echo "Test 3: Size Comparison"
	@bash bash/chapters/adv_chapter_04_serialization/compare_sizes.sh
	@echo ""
	@echo "✓ Advanced Chapter 04 tests complete!"

.PHONY: test-adv-ch05
test-adv-ch05:
	@echo "Testing Advanced Chapter 05: Error Handling & Observability"
	@echo "=================================================="
	@echo ""
	@echo "Test 1: Error Handling"
	@bash bash/chapters/adv_chapter_05_observability/test_error_handling.sh
	@echo ""
	@echo "Test 2: Dead Letter Queue Demo"
	@bash bash/chapters/adv_chapter_05_observability/demo_dlq.sh
	@echo ""
	@echo "Test 3: Metrics Monitoring"
	@bash bash/chapters/adv_chapter_05_observability/monitor_metrics.sh
	@echo ""
	@echo "✓ Advanced Chapter 05 tests complete!"

.PHONY: test-adv-ch06
test-adv-ch06:
	@echo "Testing Advanced Chapter 06: Operational Concerns"
	@echo "=================================================="
	@echo ""
	@echo "Test 1: Graceful Shutdown Demo (run for 10s)"
	@timeout 15 bash bash/chapters/adv_chapter_06_operations/demo_shutdown.sh 10 || true
	@echo ""
	@echo "Test 2: Security Configuration Guide"
	@bash bash/chapters/adv_chapter_06_operations/test_security.sh
	@echo ""
	@echo "✓ Advanced Chapter 06 tests complete!"

.PHONY: test-adv-all
test-adv-all: test-adv-ch01 test-adv-ch02 test-adv-ch03 test-adv-ch04 test-adv-ch05 test-adv-ch06
	@echo ""
	@echo "╔════════════════════════════════════════════════════════════╗"
	@echo "║  ✓ All Advanced Chapter Tests Complete!                   ║"
	@echo "╚════════════════════════════════════════════════════════════╝"
	@echo ""

# Quick test - just chapter 1 (fastest)
.PHONY: test-quick
test-quick:
	@echo "Running quick test (Advanced Chapter 01 only)..."
	@bash bash/chapters/adv_chapter_01_reliability/test_reliability.sh test-topic 10

# ==========================================
# Complete Workflow
# ==========================================

.PHONY: setup-and-test
setup-and-test:
	@echo "Complete setup and test workflow"
	@echo "=================================="
	@make kafka-apache-start
	@echo ""
	@echo "Waiting for Kafka to be fully ready..."
	@sleep 5
	@echo ""
	@if [ ! -f infra/env.local ]; then \
		echo "Creating env.local..."; \
		cp infra/env-example.local infra/env.local; \
	fi
	@echo ""
	@make test-connection
	@echo ""
	@echo "✓ Setup complete! Ready to run tests."
	@echo ""
	@echo "Run 'make test-adv-ch01' to test Advanced Chapter 01"

# ==========================================
# Convenience Targets
# ==========================================

.PHONY: test-all
test-all: python-check-connection java-reliability-test
	@echo ""
	@echo "✓ All tests completed!"

.PHONY: clean-all
clean-all: java-reliability-clean java-performance-clean
	@echo "Cleaning Python virtual environment..."
	rm -rf $(PYTHON_ENV)
	@echo "✓ All clean!"

.PHONY: help
help:
	@echo "╔════════════════════════════════════════════════════════════╗"
	@echo "║         Kafka Tutorials - Available Make Targets          ║"
	@echo "╚════════════════════════════════════════════════════════════╝"
	@echo ""
	@echo "🚀 Quick Start (Recommended):"
	@echo "  make setup-and-test              - Complete setup: start Kafka + test connection"
	@echo "  make test-adv-ch01               - Test Advanced Chapter 01 (Reliability)"
	@echo ""
	@echo "📦 Apache Kafka Cluster (Pure Apache, KRaft mode):"
	@echo "  make kafka-apache-start          - Start Apache Kafka + Kafka UI"
	@echo "  make kafka-apache-stop           - Stop Apache Kafka (keep data)"
	@echo "  make kafka-apache-clean          - Stop Apache Kafka and remove all data"
	@echo "  make kafka-apache-status         - Show status of containers"
	@echo "  make kafka-apache-logs           - Tail Kafka logs"
	@echo ""
	@echo "🔧 Confluent Platform (Full stack with Schema Registry):"
	@echo "  make kafka-start                 - Start Confluent Kafka + Schema Registry + UI"
	@echo "  make kafka-start-simple          - Start Confluent Kafka only"
	@echo "  make kafka-stop                  - Stop Confluent Kafka (keep data)"
	@echo "  make kafka-stop-all              - Stop Confluent Kafka and remove all data"
	@echo "  make kafka-status                - Show status of containers"
	@echo "  make kafka-logs                  - Tail logs from all services"
	@echo "  make kafka-restart               - Restart Confluent Kafka cluster"
	@echo ""
	@echo "🧪 Advanced Chapter Testing:"
	@echo "  make test-adv-ch01               - Test Chapter 01: Reliability & Delivery"
	@echo "  make test-adv-ch02               - Test Chapter 02: Performance & Throughput"
	@echo "  make test-adv-ch03               - Test Chapter 03: Keys & Partitioning"
	@echo "  make test-adv-ch04               - Test Chapter 04: Serialization & Schema"
	@echo "  make test-adv-ch05               - Test Chapter 05: Error Handling & Observability"
	@echo "  make test-adv-ch06               - Test Chapter 06: Operational Concerns"
	@echo "  make test-adv-all                - Run ALL advanced chapter tests (long!)"
	@echo "  make test-quick                  - Quick test (Chapter 01 only, 10 messages)"
	@echo ""
	@echo "🐍 Python Targets:"
	@echo "  make python-env                  - Create Python virtual environment"
	@echo "  make python-check-connection     - Run Python connectivity check"
	@echo ""
	@echo "☕ Java Targets (Chapter 01 - Reliability):"
	@echo "  make java-reliability-build      - Build Java reliability tests"
	@echo "  make java-reliability-test       - Run all Java reliability tests"
	@echo "  make java-reliability-integration - Run only integration tests"
	@echo "  make java-reliability-failure    - Run only failure simulation tests"
	@echo "  make java-reliability-clean      - Clean reliability build artifacts"
	@echo ""
	@echo "☕ Java Targets (Chapter 02 - Performance):"
	@echo "  make java-performance-build      - Build Java performance tests"
	@echo "  make java-performance-test       - Run all Java performance tests"
	@echo "  make java-performance-benchmark  - Run standalone benchmark application"
	@echo "  make java-performance-tests-only - Run performance benchmark tests only"
	@echo "  make java-performance-compression - Run compression comparison tests"
	@echo "  make java-performance-clean      - Clean performance build artifacts"
	@echo ""
	@echo "☕ Java Targets (Chapter 03 - Partitioning):"
	@echo "  make java-partitioning-build     - Build Java partitioning tests"
	@echo "  make java-partitioning-test      - Run all Java partitioning tests"
	@echo "  make java-partitioning-demo      - Run partitioning demonstration"
	@echo "  make java-partitioning-behavior  - Run partitioning behavior tests"
	@echo "  make java-partitioning-hotpartition - Run hot partition tests"
	@echo "  make java-partitioning-clean     - Clean partitioning build artifacts"
	@echo ""
	@echo "☕ Java Targets (Chapter 04 - Serialization):"
	@echo "  make java-serialization-build    - Build + generate Avro classes"
	@echo "  make java-serialization-test     - Run all Java serialization tests"
	@echo "  make java-serialization-demo     - Run serialization demonstration"
	@echo "  make java-serialization-formats  - Run format comparison tests"
	@echo "  make java-serialization-schema   - Run schema evolution tests"
	@echo "  make java-serialization-avro     - Run Avro serialization tests"
	@echo "  make java-serialization-clean    - Clean serialization build artifacts"
	@echo ""
	@echo "☕ Java Targets (Chapter 05 - Error Handling):"
	@echo "  make java-errorhandling-build    - Build Java error handling tests"
	@echo "  make java-errorhandling-test     - Run all Java error handling tests"
	@echo "  make java-errorhandling-demo     - Run error handling demonstration"
	@echo "  make java-errorhandling-dlq      - Run DLQ demonstration"
	@echo "  make java-errorhandling-metrics  - Run metrics demonstration"
	@echo "  make java-errorhandling-error-tests    - Run error handling tests"
	@echo "  make java-errorhandling-dlq-tests      - Run DLQ tests"
	@echo "  make java-errorhandling-metrics-tests  - Run metrics tests"
	@echo "  make java-errorhandling-clean    - Clean error handling build artifacts"
	@echo ""
	@echo "☕ Java Targets (Chapter 07 - Circuit Breaker):"
	@echo "  make java-circuitbreaker-build   - Build Java circuit breaker tests"
	@echo "  make java-circuitbreaker-test    - Run all Java circuit breaker tests"
	@echo "  make java-circuitbreaker-demo    - Run circuit breaker demonstration"
	@echo "  make java-circuitbreaker-clean   - Clean circuit breaker build artifacts"
	@echo ""
	@echo "🐚 Bash Advanced Chapter Tests:"
	@echo "  make test-adv-ch01               - Test reliability & delivery guarantees"
	@echo "  make test-adv-ch02               - Test performance & throughput"
	@echo "  make test-adv-ch03               - Test keys, partitioning & ordering"
	@echo "  make test-adv-ch07               - Test circuit breaker pattern"
	@echo ""
	@echo "🌊 Kafka Streams Targets:"
	@echo "  make streams-build-all           - Build all Streams chapters"
	@echo "  make streams-test-all            - Test all Streams chapters"
	@echo "  make streams-clean-all           - Clean all Streams chapters"
	@echo ""
	@echo "  make streams-ch01-demo           - Ch 01: Word Count demo"
	@echo "  make streams-ch02-filter         - Ch 02: Filter/Map demo"
	@echo "  make streams-ch03-demo           - Ch 03: KTable demo"
	@echo "  make streams-ch04-stream         - Ch 04: Stream-Stream Join demo"
	@echo "  make streams-ch05-demo           - Ch 05: Windowing demo"
	@echo "  make streams-ch06-demo           - Ch 06: Aggregations demo"
	@echo "  make streams-ch07-demo           - Ch 07: State Stores demo"
	@echo "  make streams-ch08-demo           - Ch 08: Topology demo"
	@echo "  make streams-ch09-demo           - Ch 09: Exactly-Once demo"
	@echo "  make streams-ch10-demo           - Ch 10: ksqlDB client demo"
	@echo ""
	@echo "  make ksqldb-start                - Start ksqlDB server"
	@echo "  make ksqldb-cli                  - Connect to ksqlDB CLI"
	@echo "  make ksqldb-stop                 - Stop ksqlDB"
	@echo ""
	@echo "🛠️  Utility Targets:"
	@echo "  make test-connection             - Test Kafka connection"
	@echo "  make test-all                    - Run all tests (Python + Java)"
	@echo "  make clean-all                   - Clean all build artifacts"
	@echo "  make help                        - Show this help message"
	@echo ""
	@echo "📖 Complete Workflow Example:"
	@echo "  1. make setup-and-test           - Start Kafka and verify"
	@echo "  2. make test-adv-ch01            - Test reliability chapter"
	@echo "  3. make test-adv-ch02            - Test performance chapter"
	@echo "  4. make kafka-apache-stop        - Stop when done"
	@echo ""
	@echo "💡 Tips:"
	@echo "  • Apache Kafka is recommended for the tutorials (pure Apache)"
	@echo "  • Kafka UI available at http://localhost:8080"
	@echo "  • Each test chapter takes 2-5 minutes"
	@echo "  • Use 'make test-quick' for fastest validation"
