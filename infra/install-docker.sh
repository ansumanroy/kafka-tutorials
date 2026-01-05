#!/usr/bin/env bash
set -euo pipefail

# Install Docker and Docker Compose on Amazon Linux 2 or Ubuntu
# Usage: ./install-docker.sh [--skip-compose] [--user <username>]

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $*"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $*"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $*" >&2
}

# Parse arguments
SKIP_COMPOSE=false
DOCKER_USER=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --skip-compose)
            SKIP_COMPOSE=true
            shift
            ;;
        --user)
            DOCKER_USER="$2"
            shift 2
            ;;
        *)
            log_error "Unknown option: $1"
            echo "Usage: $0 [--skip-compose] [--user <username>]"
            exit 1
            ;;
    esac
done

# Detect OS
if [[ -f /etc/os-release ]]; then
    . /etc/os-release
    OS=$ID
    OS_VERSION=$VERSION_ID
else
    log_error "Cannot detect OS version"
    exit 1
fi

log_info "Detected OS: $OS $OS_VERSION"

# Check if running as root
if [[ $EUID -ne 0 ]]; then
    log_error "This script must be run as root (use sudo)"
    exit 1
fi

# Install Docker
if command -v docker >/dev/null 2>&1; then
    DOCKER_VERSION=$(docker --version)
    log_info "Docker already installed: $DOCKER_VERSION"
else
    log_info "Installing Docker..."
    
    case $OS in
        amzn|amazon-linux|rhel|centos)
            # Amazon Linux 2 or RHEL/CentOS
            log_info "Installing Docker on Amazon Linux 2 / RHEL / CentOS"
            
            # Remove old versions
            yum remove -y docker docker-client docker-client-latest \
                docker-common docker-latest docker-latest-logrotate \
                docker-logrotate docker-engine 2>/dev/null || true
            
            # Install required packages
            yum install -y yum-utils device-mapper-persistent-data lvm2
            
            # Add Docker repository
            yum-config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo
            
            # Install Docker CE
            yum install -y docker-ce docker-ce-cli containerd.io
            
            # Start and enable Docker
            systemctl start docker
            systemctl enable docker
            
            log_info "Docker installed successfully"
            ;;
            
        ubuntu|debian)
            # Ubuntu or Debian
            log_info "Installing Docker on Ubuntu / Debian"
            
            # Update package index
            apt-get update
            
            # Remove old versions
            apt-get remove -y docker docker-engine docker.io containerd runc 2>/dev/null || true
            
            # Install prerequisites
            apt-get install -y \
                ca-certificates \
                curl \
                gnupg \
                lsb-release
            
            # Add Docker's official GPG key
            install -m 0755 -d /etc/apt/keyrings
            curl -fsSL https://download.docker.com/linux/${OS}/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
            chmod a+r /etc/apt/keyrings/docker.gpg
            
            # Set up repository
            echo \
              "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/${OS} \
              $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null
            
            # Install Docker
            apt-get update
            apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
            
            # Start and enable Docker
            systemctl start docker
            systemctl enable docker
            
            log_info "Docker installed successfully"
            ;;
            
        *)
            log_error "Unsupported OS: $OS"
            log_error "Please install Docker manually"
            exit 1
            ;;
    esac
fi

# Verify Docker installation
if docker --version >/dev/null 2>&1; then
    DOCKER_VERSION=$(docker --version)
    log_info "Docker verification: $DOCKER_VERSION"
    
    # Test Docker
    if docker run --rm hello-world >/dev/null 2>&1; then
        log_info "Docker test: SUCCESS"
    else
        log_warn "Docker test failed, but installation completed"
    fi
else
    log_error "Docker installation failed"
    exit 1
fi

# Install Docker Compose (v2)
if [[ "$SKIP_COMPOSE" == "false" ]]; then
    if command -v docker-compose >/dev/null 2>&1 && docker-compose --version | grep -q "version 2"; then
        COMPOSE_VERSION=$(docker-compose --version)
        log_info "Docker Compose v2 already installed: $COMPOSE_VERSION"
    elif docker compose version >/dev/null 2>&1; then
        COMPOSE_VERSION=$(docker compose version)
        log_info "Docker Compose v2 already installed: $COMPOSE_VERSION"
    else
        log_info "Installing Docker Compose v2..."
        
        # Docker Compose v2 is installed as a plugin on newer systems
        # For older systems or standalone installation
        COMPOSE_VERSION=$(curl -s https://api.github.com/repos/docker/compose/releases/latest | grep 'tag_name' | cut -d'"' -f4)
        
        case $OS in
            amzn|amazon-linux|rhel|centos)
                # Install as standalone binary
                mkdir -p /usr/local/lib/docker/cli-plugins
                curl -SL "https://github.com/docker/compose/releases/download/${COMPOSE_VERSION}/docker-compose-linux-x86_64" \
                    -o /usr/local/lib/docker/cli-plugins/docker-compose
                chmod +x /usr/local/lib/docker/cli-plugins/docker-compose
                
                # Create symlink for backward compatibility
                ln -sf /usr/local/lib/docker/cli-plugins/docker-compose /usr/local/bin/docker-compose
                ;;
                
            ubuntu|debian)
                # Should already be installed with docker-compose-plugin, but verify
                if ! docker compose version >/dev/null 2>&1; then
                    # Fallback to standalone installation
                    mkdir -p /usr/local/lib/docker/cli-plugins
                    curl -SL "https://github.com/docker/compose/releases/download/${COMPOSE_VERSION}/docker-compose-linux-$(uname -m)" \
                        -o /usr/local/lib/docker/cli-plugins/docker-compose
                    chmod +x /usr/local/lib/docker/cli-plugins/docker-compose
                    ln -sf /usr/local/lib/docker/cli-plugins/docker-compose /usr/local/bin/docker-compose
                fi
                ;;
        esac
        
        log_info "Docker Compose v2 installed successfully"
    fi
    
    # Verify Docker Compose
    if docker compose version >/dev/null 2>&1; then
        COMPOSE_VERSION=$(docker compose version)
        log_info "Docker Compose verification: $COMPOSE_VERSION"
    elif docker-compose --version >/dev/null 2>&1; then
        COMPOSE_VERSION=$(docker-compose --version)
        log_info "Docker Compose verification: $COMPOSE_VERSION"
    else
        log_warn "Docker Compose verification failed, but installation completed"
    fi
else
    log_info "Skipping Docker Compose installation (--skip-compose flag)"
fi

# Add user to docker group (if specified)
if [[ -n "$DOCKER_USER" ]]; then
    if id "$DOCKER_USER" &>/dev/null; then
        log_info "Adding user '$DOCKER_USER' to docker group..."
        usermod -aG docker "$DOCKER_USER"
        log_info "User '$DOCKER_USER' added to docker group"
        log_warn "User needs to log out and log back in for changes to take effect"
    else
        log_warn "User '$DOCKER_USER' does not exist, skipping group addition"
    fi
fi

# Summary
log_info "═══════════════════════════════════════════════════════════"
log_info "Installation Complete"
log_info "═══════════════════════════════════════════════════════════"
log_info "Docker: $(docker --version)"
if [[ "$SKIP_COMPOSE" == "false" ]]; then
    if docker compose version >/dev/null 2>&1; then
        log_info "Docker Compose: $(docker compose version)"
    elif docker-compose --version >/dev/null 2>&1; then
        log_info "Docker Compose: $(docker-compose --version)"
    fi
fi
log_info ""
log_info "Docker service status: $(systemctl is-active docker)"
log_info "Docker service enabled: $(systemctl is-enabled docker)"
log_info ""
if [[ -n "$DOCKER_USER" ]]; then
    log_info "User '$DOCKER_USER' added to docker group"
    log_info "Note: User must log out and log back in to use Docker without sudo"
fi
log_info ""
log_info "Test Docker with: docker run hello-world"
if [[ "$SKIP_COMPOSE" == "false" ]]; then
    log_info "Test Docker Compose with: docker compose version"
fi

