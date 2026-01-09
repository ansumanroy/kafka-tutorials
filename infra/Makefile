.PHONY: help install-docker start stop restart status logs logs-follow logs-redpanda logs-kafka-ui logs-conduktor clean get-ec2-ip create-sasl-user list-sasl-users install-docker-check

# Default target
help:
	@echo "Available targets:"
	@echo "  install-docker     - Install Docker and Docker Compose (requires sudo)"
	@echo "  start              - Start Kafka UI docker-compose stack"
	@echo "  stop               - Stop Kafka UI docker-compose stack"
	@echo "  restart            - Restart Kafka UI docker-compose stack"
	@echo "  status             - Show status of all services"
	@echo "  logs               - View logs from all services"
	@echo "  logs-follow        - Follow logs from all services"
	@echo "  clean              - Stop and remove all containers, volumes, and networks"
	@echo "  install-docker-check - Check if Docker and Docker Compose are installed"

# Variables
COMPOSE_FILE = docker-compose-kafkaui.yml
SCRIPT_DIR = $(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))

# Install Docker and Docker Compose
install-docker:
	@echo "Installing Docker and Docker Compose..."
	@sudo bash $(SCRIPT_DIR)/install-docker.sh
	@echo ""
	@echo "Installation complete! You may need to log out and back in for group changes to take effect."

# Check if Docker and Docker Compose are installed
install-docker-check:
	@echo "Checking Docker and Docker Compose installation..."
	@if command -v docker >/dev/null 2>&1; then \
		echo "✓ Docker is installed: $$(docker --version)"; \
	else \
		echo "✗ Docker is NOT installed. Run 'make install-docker' to install."; \
		exit 1; \
	fi
	@if command -v docker compose >/dev/null 2>&1 || command -v docker-compose >/dev/null 2>&1; then \
		if command -v docker compose >/dev/null 2>&1; then \
			echo "✓ Docker Compose is installed: $$(docker compose version)"; \
		else \
			echo "✓ Docker Compose is installed: $$(docker-compose --version)"; \
		fi; \
	else \
		echo "✗ Docker Compose is NOT installed. Run 'make install-docker' to install."; \
		exit 1; \
	fi

# Start Kafka UI stack
start: install-docker-check
	@echo "Starting Kafka UI docker-compose stack..."
	@cd $(SCRIPT_DIR) && docker compose -f $(COMPOSE_FILE) up -d
	@echo ""
	@echo "Services are starting up. Use 'make status' to check service status."
	@echo "Use 'make logs' to view logs."
	@echo ""
	@echo "Access points (when running on EC2, replace localhost with EC2 public IP):"
	@echo "  - Conduktor Console: http://localhost:8080"
	@echo "  - Kafka UI: http://localhost:8090"
	@echo "  - Kafdrop: http://localhost:9000"
	@echo "  - Kafka (SASL_PLAINTEXT): <EC2_PUBLIC_IP>:19093"
	@echo ""
	@echo "Default SASL credentials: admin/admin"
	@echo "See README-EC2-DEPLOYMENT.md for connection details."

# Stop Kafka UI stack
stop:
	@echo "Stopping Kafka UI docker-compose stack..."
	@cd $(SCRIPT_DIR) && docker compose -f $(COMPOSE_FILE) stop
	@echo "Services stopped."

# Restart Kafka UI stack
restart: stop start

# Show status of services
status:
	@cd $(SCRIPT_DIR) && docker compose -f $(COMPOSE_FILE) ps

# View logs from all services
logs:
	@cd $(SCRIPT_DIR) && docker compose -f $(COMPOSE_FILE) logs

# Follow logs from all services
logs-follow:
	@cd $(SCRIPT_DIR) && docker compose -f $(COMPOSE_FILE) logs -f

# View logs from a specific service
logs-redpanda:
	@cd $(SCRIPT_DIR) && docker compose -f $(COMPOSE_FILE) logs redpanda-0

logs-kafka-ui:
	@cd $(SCRIPT_DIR) && docker compose -f $(COMPOSE_FILE) logs kafka-ui

logs-conduktor:
	@cd $(SCRIPT_DIR) && docker compose -f $(COMPOSE_FILE) logs conduktor-console

# Clean: stop and remove all containers, volumes, and networks
# Usage: make clean CONFIRM=true (to skip confirmation prompt)
clean:
	@if [ "$(CONFIRM)" != "true" ]; then \
		echo "WARNING: This will remove all containers, volumes, and networks!"; \
		echo "Run 'make clean CONFIRM=true' to proceed without confirmation."; \
		exit 1; \
	fi
	@echo "Stopping and removing all containers, volumes, and networks..."
	@cd $(SCRIPT_DIR) && docker compose -f $(COMPOSE_FILE) down -v
	@echo "Cleanup complete."

# Get EC2 public IP (helper target)
get-ec2-ip:
	@bash $(SCRIPT_DIR)/scripts/get-ec2-public-ip.sh

# Create SASL user (helper target)
create-sasl-user:
	@echo "Creating SASL user (default: admin/admin)..."
	@docker exec -it redpanda-0 rpk acl user create admin \
		--new-password admin \
		--mechanism plain || echo "User may already exist. Use 'make list-sasl-users' to check."

# List SASL users (helper target)
list-sasl-users:
	@docker exec -it redpanda-0 rpk acl user list

