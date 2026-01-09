# EC2 Deployment Guide - Kafka with SASL_PLAINTEXT Authentication

This guide explains how to deploy the Kafka UI docker-compose stack on an EC2 instance with external access using SASL_PLAINTEXT authentication.

## Overview

The `docker-compose-kafkaui.yml` file is configured to:
- Auto-detect EC2 public IP from metadata service
- Expose Kafka with SASL_PLAINTEXT authentication on port 19093
- Use PLAIN SASL mechanism for username/password authentication
- Maintain backward compatibility with PLAINTEXT on port 19092 (localhost only)

## Prerequisites

1. **EC2 Instance** running Amazon Linux 2 or Ubuntu
2. **Docker and Docker Compose** installed
3. **Security Group** configured to allow inbound traffic on:
   - Port 19093 (SASL_PLAINTEXT - external access)
   - Port 8080 (Conduktor Console)
   - Port 8090 (Kafka UI)
   - Port 9000 (Kafdrop)

## Quick Start

### 1. Clone Repository and Navigate to Infra Directory

```bash
cd /home/ec2-user
git clone <your-repo-url> kafka-tutorials
cd kafka-tutorials/infra
```

### 2. Install Docker and Docker Compose (if not already installed)

```bash
# Using Makefile (recommended)
make install-docker

# Or manually
sudo bash install-docker.sh
```

### 3. Get Your EC2 Public IP

```bash
# Option 1: Use the helper script
./scripts/get-ec2-public-ip.sh

# Option 2: Use curl directly
curl http://169.254.169.254/latest/meta-data/public-ipv4

# Option 3: Use AWS CLI
aws ec2 describe-instances --instance-ids $(curl -s http://169.254.169.254/latest/meta-data/instance-id) --query 'Reservations[0].Instances[0].PublicIpAddress' --output text
```

### 4. Configure SASL Credentials (Optional)

By default, the system uses:
- Username: `admin`
- Password: `admin`

To customize, create a `.env` file:

```bash
cat > .env <<EOF
REDPANDA_SASL_USER=myuser
REDPANDA_SASL_PASSWORD=mypassword
EOF
```

Or export environment variables:

```bash
export REDPANDA_SASL_USER=myuser
export REDPANDA_SASL_PASSWORD=mypassword
```

### 4. Start the Services

**Option A: Using Makefile (Recommended)**

```bash
# Install Docker and Docker Compose first (if not already installed)
make install-docker

# Start the services
make start

# Check status
make status

# View logs
make logs
```

**Option B: Using Docker Compose Directly**

```bash
docker compose -f docker-compose-kafkaui.yml up -d
```

### 5. Verify Services are Running

**Using Makefile:**
```bash
make status
```

**Or using Docker Compose:**
```bash
docker compose -f docker-compose-kafkaui.yml ps
```

Check logs to see the detected EC2 public IP:

```bash
docker compose -f docker-compose-kafkaui.yml logs redpanda-0 | grep "Using EC2 public IP"
```

### 6. Create SASL Users (After First Startup)

Redpanda requires users to be created using `rpk`. 

**Using Makefile:**
```bash
# Create default admin user
make create-sasl-user

# List all users
make list-sasl-users
```

**Or manually:**
```bash
docker exec -it redpanda-0 rpk acl user create admin \
  --new-password admin \
  --mechanism plain
```

For additional users:

```bash
docker exec -it redpanda-0 rpk acl user create <username> \
  --new-password <password> \
  --mechanism plain
```

## Connection Information

### External Clients (SASL_PLAINTEXT)

**Bootstrap Servers:**
```
<EC2_PUBLIC_IP>:19093
```

**Connection Settings:**
- Security Protocol: `SASL_PLAINTEXT`
- SASL Mechanism: `PLAIN`
- Username: `admin` (or your custom username)
- Password: `admin` (or your custom password)

### Example: Kafka Console Producer

```bash
kafka-console-producer.sh \
  --bootstrap-server <EC2_PUBLIC_IP>:19093 \
  --topic test-topic \
  --producer-property security.protocol=SASL_PLAINTEXT \
  --producer-property sasl.mechanism=PLAIN \
  --producer-property sasl.jaas.config="org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"admin\";"
```

### Example: Kafka Console Consumer

```bash
kafka-console-consumer.sh \
  --bootstrap-server <EC2_PUBLIC_IP>:19093 \
  --topic test-topic \
  --from-beginning \
  --consumer-property security.protocol=SASL_PLAINTEXT \
  --consumer-property sasl.mechanism=PLAIN \
  --consumer-property sasl.jaas.config="org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"admin\";"
```

### Example: Java Producer Configuration

```java
Properties props = new Properties();
props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "<EC2_PUBLIC_IP>:19093");
props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
props.put(SaslConfigs.SASL_JAAS_CONFIG, 
    "org.apache.kafka.common.security.plain.PlainLoginModule required " +
    "username=\"admin\" password=\"admin\";");
```

### Example: Python Client (kafka-python)

```python
from kafka import KafkaProducer
from kafka.errors import KafkaError

producer = KafkaProducer(
    bootstrap_servers=['<EC2_PUBLIC_IP>:19093'],
    security_protocol='SASL_PLAINTEXT',
    sasl_mechanism='PLAIN',
    sasl_plain_username='admin',
    sasl_plain_password='admin',
    value_serializer=lambda v: v.encode('utf-8')
)
```

## Security Group Configuration

Ensure your EC2 security group allows inbound traffic:

| Port | Protocol | Source | Description |
|------|----------|--------|-------------|
| 19093 | TCP | 0.0.0.0/0 or specific IPs | SASL_PLAINTEXT Kafka access |
| 8080 | TCP | 0.0.0.0/0 or specific IPs | Conduktor Console |
| 8090 | TCP | 0.0.0.0/0 or specific IPs | Kafka UI |
| 9000 | TCP | 0.0.0.0/0 or specific IPs | Kafdrop |

**Security Best Practice:** Restrict source IPs to your specific IP ranges instead of `0.0.0.0/0` for production deployments.

## Accessing UIs

Once services are running:

- **Conduktor Console**: `http://<EC2_PUBLIC_IP>:8080`
- **Kafka UI**: `http://<EC2_PUBLIC_IP>:8090`
- **Kafdrop**: `http://<EC2_PUBLIC_IP>:9000`

## Troubleshooting

### Cannot Connect from External Client

1. **Check Security Group**: Ensure port 19093 is open
   ```bash
   aws ec2 describe-security-groups --group-ids <your-sg-id>
   ```

2. **Verify EC2 Public IP Detection**:
   ```bash
   docker compose -f docker-compose-kafkaui.yml logs redpanda-0 | grep "Using EC2 public IP"
   ```

3. **Check if Redpanda is Listening**:
   ```bash
   docker exec redpanda-0 rpk cluster info
   ```

4. **Test Connectivity from EC2**:
   ```bash
   telnet localhost 19093
   ```

### Authentication Errors

1. **Verify User Exists**:
   ```bash
   docker exec -it redpanda-0 rpk acl user list
   ```

2. **Create/Update User**:
   ```bash
   docker exec -it redpanda-0 rpk acl user create admin \
     --new-password admin \
     --mechanism plain
   ```

3. **Check Credentials**: Ensure username/password match in your client configuration

### Services Not Starting

1. **Check Logs**:
   ```bash
   docker compose -f docker-compose-kafkaui.yml logs
   ```

2. **Verify Port Availability**:
   ```bash
   sudo netstat -tlnp | grep -E ':(19093|8080|8090|9000)'
   ```

3. **Check Docker Resources**:
   ```bash
   docker system df
   docker stats
   ```

## Managing Services

**Using Makefile:**
```bash
# Stop services (keeps data)
make stop

# Restart services
make restart

# View logs
make logs

# Follow logs in real-time
make logs-follow

# View logs for specific service
make logs-redpanda
make logs-kafka-ui
make logs-conduktor

# Stop and remove all data (requires CONFIRM=true)
make clean CONFIRM=true

# Check service status
make status
```

**Or using Docker Compose:**
```bash
# Stop services (keeps data)
docker compose -f docker-compose-kafkaui.yml down

# Stop and remove all data
docker compose -f docker-compose-kafkaui.yml down -v

# View logs
docker compose -f docker-compose-kafkaui.yml logs -f
```

## Notes

- **SASL_PLAINTEXT** transmits credentials in plain text over the network. Only use this for development/testing or within trusted networks.
- For production, consider using **SASL_SSL** with proper certificates.
- The EC2 public IP is auto-detected on container startup. If your IP changes, restart the containers.
- Default credentials (`admin/admin`) should be changed for any production deployment.

## Additional Resources

- [Redpanda SASL Documentation](https://docs.redpanda.com/docs/manage/security/authentication/)
- [Kafka SASL Configuration](https://kafka.apache.org/documentation/#security_sasl)
- [AWS EC2 Metadata Service](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-instance-metadata.html)

