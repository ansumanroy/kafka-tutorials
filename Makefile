PYTHON_ENV ?= .venv
PYTHON ?= python3
JAVA_ADV_RELIABILITY_DIR = java/adv_chapter_01_reliability

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
# Convenience Targets
# ==========================================

.PHONY: test-all
test-all: python-check-connection java-reliability-test
	@echo ""
	@echo "✓ All tests completed!"

.PHONY: clean-all
clean-all: java-reliability-clean
	@echo "Cleaning Python virtual environment..."
	rm -rf $(PYTHON_ENV)
	@echo "✓ All clean!"

.PHONY: help
help:
	@echo "Kafka Tutorials - Available Make Targets"
	@echo ""
	@echo "Kafka Cluster Management (Docker):"
	@echo "  make kafka-start                 - Start full Kafka cluster (Kafka + Schema Registry + UI)"
	@echo "  make kafka-start-simple          - Start simple Kafka only (no Schema Registry)"
	@echo "  make kafka-stop                  - Stop Kafka cluster (keep data)"
	@echo "  make kafka-stop-all              - Stop Kafka cluster and remove all data"
	@echo "  make kafka-status                - Show status of running containers"
	@echo "  make kafka-logs                  - Tail logs from all services"
	@echo "  make kafka-logs-kafka            - Tail logs from Kafka broker only"
	@echo "  make kafka-logs-schema-registry  - Tail logs from Schema Registry only"
	@echo "  make kafka-restart               - Restart Kafka cluster"
	@echo ""
	@echo "Python Targets:"
	@echo "  make python-env                  - Create Python virtual environment"
	@echo "  make python-check-connection     - Run Python connectivity check"
	@echo ""
	@echo "Java Targets:"
	@echo "  make java-reliability-build      - Build Java reliability tests"
	@echo "  make java-reliability-test       - Run all Java reliability tests"
	@echo "  make java-reliability-integration - Run only integration tests"
	@echo "  make java-reliability-failure    - Run only failure simulation tests"
	@echo "  make java-reliability-clean      - Clean Java build artifacts"
	@echo ""
	@echo "Convenience Targets:"
	@echo "  make test-all                    - Run all tests (Python + Java)"
	@echo "  make clean-all                   - Clean all build artifacts"
	@echo "  make help                        - Show this help message"
	@echo ""
	@echo "Quick Start:"
	@echo "  1. make kafka-start              - Start Kafka cluster"
	@echo "  2. cp infra/env-example.local infra/env.local"
	@echo "  3. source infra/env.local"
	@echo "  4. make java-reliability-test    - Run tests"
