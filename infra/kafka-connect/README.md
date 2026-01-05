# Kafka Backup and Restore with Kafka Connect

A complete solution for backing up and restoring Kafka topics from MSK clusters to S3, preserving exact offsets and topic configurations.

## Features

- **Full Topic Backup**: Backs up all topics or specific topics from MSK cluster
- **Exact Offset Preservation**: Maintains original message offsets for restoration
- **Topic Configuration Backup**: Exports and restores topic settings (partitions, replication, retention, etc.)
- **S3 Storage**: Stores backups in S3 with organized directory structure
- **Scheduled Backups**: Supports cron and EventBridge scheduling
- **Consumer Group Offset Restoration**: Restores consumer group offsets after message restoration
- **Open Source**: Uses Apache Camel Kafka Connector (free and open source)

## Architecture

```
MSK Cluster → Kafka Connect → S3 Bucket
                ↓
          Topic Configs + Messages + Offset Metadata

Restore:
S3 Bucket → Restore Scripts → MSK Cluster
           (Topics + Messages + Offsets)
```

## Prerequisites

1. **AWS Account** with:
   - MSK cluster running
   - S3 bucket for backups
   - VPC and subnet access

2. **Software Requirements**:
   - Terraform >= 1.0
   - AWS CLI v2
   - Docker and Docker Compose
   - Python 3.9+ with `kafka-python` and `boto3`
   - `jq` for JSON processing
   - Kafka CLI tools (for topic operations)

3. **Permissions**:
   - EC2 instance with access to MSK cluster
   - S3 read/write permissions
   - Ability to create IAM roles and security groups

## Quick Start

### 1. Deploy Infrastructure

```bash
cd infra/kafka-connect/terraform
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars with your values

terraform init
terraform plan
terraform apply
```

This creates:
- EC2 instance for Kafka Connect
- IAM roles with S3 and MSK permissions
- Security groups

### 2. Configure Environment

On the EC2 instance:

```bash
# Clone this repository or copy files
cd /opt/kafka-connect

# Set environment variables
export KAFKA_BOOTSTRAP_SERVERS="your-msk-endpoint:9092"
export KAFKA_SECURITY_PROTOCOL="PLAINTEXT"  # or SASL_SSL, SASL_PLAINTEXT
export KAFKA_SASL_MECHANISM=""  # if using SASL
export KAFKA_SASL_USERNAME=""
export KAFKA_SASL_PASSWORD=""
export S3_BUCKET="your-backup-bucket"
export AWS_REGION="us-east-1"

# Create env file for scripts
cat > /opt/kafka-connect/.env <<EOF
KAFKA_BOOTSTRAP_SERVERS=$KAFKA_BOOTSTRAP_SERVERS
KAFKA_SECURITY_PROTOCOL=$KAFKA_SECURITY_PROTOCOL
KAFKA_SASL_MECHANISM=$KAFKA_SASL_MECHANISM
KAFKA_SASL_USERNAME=$KAFKA_SASL_USERNAME
KAFKA_SASL_PASSWORD=$KAFKA_SASL_PASSWORD
S3_BUCKET=$S3_BUCKET
AWS_REGION=$AWS_REGION
EOF
```

### 3. Deploy Kafka Connect

```bash
# Copy docker-compose.yml and configuration files
cd /opt/kafka-connect

# Update docker-compose.yml with your MSK bootstrap servers
# Update connect-distributed.properties

# Start Kafka Connect
docker-compose up -d

# Verify it's running
curl http://localhost:8083/connectors
```

### 4. Install Python Dependencies

```bash
pip3 install kafka-python boto3
```

## Usage

### Backup

#### Backup All Topics

```bash
cd /opt/kafka-connect/scripts

# Set Connect REST URL
export CONNECT_REST_URL="http://localhost:8083"

# Run backup
./backup.sh "$CONNECT_REST_URL" "$S3_BUCKET"
```

#### Backup Specific Topic

```bash
./backup.sh "$CONNECT_REST_URL" "$S3_BUCKET" "my-topic"
```

#### Monitor Backup Progress

```bash
# Check connector status
curl "$CONNECT_REST_URL/connectors"

# Check specific connector
curl "$CONNECT_REST_URL/connectors/camel-s3-sink-my-topic/status"

# View connector tasks
curl "$CONNECT_REST_URL/connectors/camel-s3-sink-my-topic/tasks"
```

### Restore

#### Restore All Topics

```bash
cd /opt/kafka-connect/scripts

BACKUP_ID="20240101_120000"  # From your backup

# Full restore (topics + messages + offsets)
./restore.sh "$S3_BUCKET" "$BACKUP_ID"

# Restore with consumer group offsets
./restore.sh "$S3_BUCKET" "$BACKUP_ID" "" "my-consumer-group"
```

#### Restore Specific Topic

```bash
./restore.sh "$S3_BUCKET" "$BACKUP_ID" "my-topic"
```

#### Restore Consumer Group Offsets

If you already restored messages and want to restore offsets:

```bash
OFFSET_MAPPINGS="/tmp/kafka-restore-20240101_120000/offset-mappings.json"
./restore-offsets.sh "$OFFSET_MAPPINGS" "my-consumer-group" "my-topic"
```

### Scheduled Backups

#### Using Cron

```bash
# Schedule daily backup at 2 AM
./schedule-backup.sh "$CONNECT_REST_URL" "$S3_BUCKET" cron "0 2 * * *"

# View scheduled jobs
crontab -l
```

#### Using EventBridge

```bash
# Create EventBridge rule (manual setup or use Terraform)
./schedule-backup.sh "$CONNECT_REST_URL" "$S3_BUCKET" eventbridge "cron(0 2 * * ? *)"
```

See `terraform/eventbridge.tf` for Terraform-based EventBridge setup.

## S3 Backup Structure

```
s3://your-bucket/
└── backups/
    └── 20240101_120000/          # Backup ID (timestamp)
        ├── topics/
        │   └── my-topic/
        │       ├── partition-0/
        │       │   ├── messages-0000000000.json.gz
        │       │   ├── messages-0000000100.json.gz
        │       │   └── offsets.json
        │       └── partition-1/
        │           └── ...
        ├── configs/
        │   ├── my-topic-config.json
        │   └── another-topic-config.json
        └── metadata/
            └── backup-manifest.json
```

## How It Works

### Backup Process

1. **Topic Discovery**: Script discovers all topics in MSK (or uses specified topic)
2. **Config Export**: Topic configurations are exported to JSON and stored in S3
3. **Connector Creation**: S3 sink connector is created for each topic via Connect REST API
4. **Message Backup**: Connector reads messages and writes to S3 with offset metadata
5. **Monitoring**: Script monitors connector status until backup completes

### Restore Process

1. **Topic Recreation**: Topics are recreated with original configurations (partitions, replication, settings)
2. **Message Restoration**: Messages are read from S3 and produced to Kafka
3. **Offset Mapping**: Original offsets are mapped to new offsets
4. **Offset Restoration**: Consumer group offsets are restored based on mapping

### Offset Preservation

Since Kafka doesn't allow directly setting producer offsets, the solution:

1. Stores offset metadata with each message in S3
2. Produces messages in order, tracking new offsets
3. Creates a mapping file of old → new offsets
4. Uses Kafka Admin API to reset consumer group offsets to restored messages

## Configuration

### Kafka Connect Configuration

Edit `connect-distributed.properties`:

```properties
bootstrap.servers=your-msk-endpoint:9092
group.id=kafka-connect-cluster
plugin.path=/kafka/connect
```

### S3 Connector Template

Edit `connectors/camel-s3-sink.json` to customize:
- S3 bucket and prefix
- Compression type
- File format
- Partitioning strategy

### Security Configuration

For SASL/SSL authentication, set environment variables:

```bash
export KAFKA_SECURITY_PROTOCOL="SASL_SSL"
export KAFKA_SASL_MECHANISM="SCRAM-SHA-512"
export KAFKA_SASL_USERNAME="username"
export KAFKA_SASL_PASSWORD="password"
```

Or use IAM authentication for MSK:

```bash
export KAFKA_SECURITY_PROTOCOL="SASL_SSL"
export KAFKA_SASL_MECHANISM="AWS_MSK_IAM"
```

## Troubleshooting

### Connector Not Starting

```bash
# Check connector status
curl "$CONNECT_REST_URL/connectors/camel-s3-sink-my-topic/status"

# Check Connect logs
docker-compose logs kafka-connect

# Verify S3 permissions
aws s3 ls s3://$S3_BUCKET/
```

### Messages Not Appearing in S3

1. Check connector tasks are running:
   ```bash
   curl "$CONNECT_REST_URL/connectors/camel-s3-sink-my-topic/tasks"
   ```

2. Verify topic has messages:
   ```bash
   kafka-console-consumer.sh --bootstrap-server $KAFKA_BOOTSTRAP_SERVERS \
     --topic my-topic --from-beginning --max-messages 1
   ```

3. Check S3 bucket permissions in IAM role

### Restore Failing

1. **Topic Already Exists**: Delete topic first or use `--if-exists update` option
2. **Offset Mappings Missing**: Ensure restore-messages.sh completed successfully
3. **S3 Access Issues**: Verify IAM role has S3 read permissions

### Performance Issues

- **Large Topics**: Backup may take time for topics with millions of messages
- **Connector Tuning**: Adjust `tasks.max` in connector configuration
- **S3 Upload Speed**: Consider using S3 Transfer Acceleration for faster uploads

## Limitations

1. **Producer Offsets**: Cannot directly restore producer offsets (only consumer group offsets)
2. **Transaction State**: Transactional producer state is not preserved
3. **Schema Registry**: If using Avro/Protobuf, schemas must be backed up separately
4. **Consumer Lag**: Large topics may take significant time to backup

## Scripts Reference

| Script | Purpose |
|--------|---------|
| `backup.sh` | Orchestrate complete backup process |
| `restore.sh` | Orchestrate complete restore process |
| `export-topic-configs.sh` | Export topic configurations to S3 |
| `create-sink-connectors.sh` | Create S3 sink connectors for topics |
| `restore-topics.sh` | Restore topic configurations |
| `restore-messages.sh` | Restore messages from S3 |
| `restore-offsets.sh` | Restore consumer group offsets |
| `schedule-backup.sh` | Set up scheduled backups |

## Additional Resources

- [Apache Camel Kafka Connector](https://camel.apache.org/camel-kafka-connector/)
- [Kafka Connect REST API](https://kafka.apache.org/documentation/#connect_rest)
- [MSK Documentation](https://docs.aws.amazon.com/msk/)

## Support

For issues or questions:
1. Check logs in `/var/log/kafka-backup.log`
2. Review Connect logs: `docker-compose logs kafka-connect`
3. Verify S3 backup structure matches expected format

## License

This solution uses open-source components and is provided as-is for backup and restore operations.

