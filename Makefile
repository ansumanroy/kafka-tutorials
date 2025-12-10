PYTHON_ENV ?= .venv
PYTHON ?= python3
JAVA_ADV_RELIABILITY_DIR = java/adv_chapter_01_reliability
JAVA_ADV_PERFORMANCE_DIR = java/adv_chapter_02_performance

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
