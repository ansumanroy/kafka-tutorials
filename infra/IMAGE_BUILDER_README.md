# EC2 Image Builder for Kafka Development Environment

This directory contains EC2 Image Builder artifacts for creating Amazon Linux 2 AMIs pre-configured with Kafka development tools.

## What's Included

The Image Builder pipeline installs and configures:

- **Python 3** - Latest Python 3 with pip
- **AWS CLI v2** - Latest AWS Command Line Interface
- **Docker** - Docker Engine with systemd service enabled
- **Docker Compose v2** - Latest Docker Compose
- **Java 11** - Amazon Corretto 11 (required for Kafka)
- **Apache Kafka** - Latest stable Kafka binaries with command-line tools
- **Kafka UI** - Provectus Kafka UI running as a Docker container

All tools are pre-configured and ready to use on instance launch.

## Files

- `imagebuilder-component.yml` - Standalone Image Builder component document
- `imagebuilder-stack.yml` - Complete CloudFormation template with IAM roles and pipeline
- `IMAGE_BUILDER_README.md` - This documentation

## Prerequisites

Before deploying the CloudFormation stack, you need:

1. **VPC and Subnet** - A VPC with a subnet that has internet access (NAT Gateway or Internet Gateway)
2. **Security Group** - A security group that allows:
   - Outbound internet access (for downloading packages)
   - SSM access (for Image Builder to communicate)
3. **Base AMI** - Amazon Linux 2 AMI ID for your region (default provided)

### Security Group Requirements

Your security group should allow:

**Outbound Rules:**
- All traffic to 0.0.0.0/0 (for downloading packages and tools)

**Inbound Rules:**
- None required (Image Builder uses SSM)

## Deployment Options

### Option 1: CloudFormation Stack (Recommended)

Deploy the complete stack with all resources:

```bash
# Edit parameters as needed
aws cloudformation create-stack \
  --stack-name kafka-dev-image-builder \
  --template-body file://imagebuilder-stack.yml \
  --parameters \
    ParameterKey=VpcId,ParameterValue=vpc-xxxxx \
    ParameterKey=SubnetId,ParameterValue=subnet-xxxxx \
    ParameterKey=SecurityGroupId,ParameterValue=sg-xxxxx \
    ParameterKey=InstanceType,ParameterValue=t3.medium \
    ParameterKey=NotificationEmail,ParameterValue=your-email@example.com \
  --capabilities CAPABILITY_NAMED_IAM \
  --region us-east-1
```

**Parameters:**

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| VpcId | Yes | - | VPC ID for build instances |
| SubnetId | Yes | - | Subnet with internet access |
| SecurityGroupId | Yes | - | Security group for build instances |
| InstanceType | No | t3.medium | Instance type for building |
| TargetRegions | No | us-east-1 | Regions to copy AMI to |
| NotificationEmail | No | - | Email for build notifications |
| BaseImageId | No | Latest AL2 | Base Amazon Linux 2 AMI |
| ComponentVersion | No | 1.0.0 | Component version number |

### Option 2: Standalone Component

Use the component in your existing Image Builder pipeline:

1. Upload the component:

```bash
aws imagebuilder create-component \
  --name KafkaDevEnvironment \
  --platform Linux \
  --version 1.0.0 \
  --data file://imagebuilder-component.yml \
  --supported-os-versions "Amazon Linux 2" \
  --region us-east-1
```

2. Reference the component ARN in your existing image recipe.

## Starting a Build

After deploying the CloudFormation stack, start a build:

```bash
# Get the pipeline ARN from stack outputs
PIPELINE_ARN=$(aws cloudformation describe-stacks \
  --stack-name kafka-dev-image-builder \
  --query 'Stacks[0].Outputs[?OutputKey==`ImagePipelineArn`].OutputValue' \
  --output text)

# Start the build
aws imagebuilder start-image-pipeline-execution \
  --image-pipeline-arn $PIPELINE_ARN \
  --region us-east-1
```

The build typically takes **20-30 minutes**.

## Monitoring Build Progress

### View Build Status

```bash
aws imagebuilder list-image-pipeline-images \
  --image-pipeline-arn $PIPELINE_ARN \
  --region us-east-1
```

### View Build Logs

1. Go to AWS Console → EC2 Image Builder → Image pipelines
2. Select your pipeline
3. Click on the "Images" tab
4. Select the build in progress
5. View execution details and logs

Logs are also stored in the S3 bucket created by the stack:
- Bucket: `{StackName}-components-{AccountId}`
- Prefix: `logs/`

### SNS Notifications

If you provided a `NotificationEmail` parameter, you'll receive email notifications for:
- Build started
- Build completed
- Build failed

## Using the AMI

Once the build completes successfully:

1. **Find the AMI:**

```bash
aws ec2 describe-images \
  --owners self \
  --filters "Name=name,Values=kafka-dev-*" \
  --query 'Images | sort_by(@, &CreationDate) | [-1].[ImageId, Name, CreationDate]' \
  --output table
```

2. **Launch an instance:**

```bash
AMI_ID="ami-xxxxx"  # From previous command

aws ec2 run-instances \
  --image-id $AMI_ID \
  --instance-type t3.medium \
  --key-name your-key-pair \
  --subnet-id subnet-xxxxx \
  --security-group-ids sg-xxxxx \
  --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=kafka-dev-instance}]'
```

3. **Connect to the instance:**

```bash
ssh -i your-key.pem ec2-user@<instance-public-ip>
```

## Post-Launch Verification

After launching an instance from the AMI, verify the installation:

```bash
# Python 3
python3 --version

# AWS CLI v2
aws --version

# Docker
docker --version
systemctl status docker

# Docker Compose
docker-compose --version

# Java
java -version

# Kafka tools
kafka-topics.sh --version
kafka-console-producer.sh --version
kafka-console-consumer.sh --version

# Check Kafka UI service
systemctl status kafka-ui
```

## Using Kafka UI

Kafka UI is installed and configured to start automatically on boot.

### Start Kafka UI

```bash
# Kafka UI starts automatically on boot
# To manually control it:
sudo systemctl start kafka-ui
sudo systemctl stop kafka-ui
sudo systemctl restart kafka-ui
```

### Access Kafka UI

1. Ensure your security group allows inbound TCP on port 8080
2. Access in browser: `http://<instance-public-ip>:8080`

### Configure Kafka Cluster in UI

Since Kafka UI is installed with dynamic configuration enabled, you can add clusters via the UI:

1. Open Kafka UI in browser
2. Click "Configure New Cluster"
3. Enter cluster details (bootstrap servers, authentication, etc.)
4. Save configuration

Alternatively, configure clusters via docker-compose:

```bash
# Edit the docker-compose file
sudo nano /opt/kafka-ui/docker-compose.yml

# Add your Kafka cluster configuration
# See: https://docs.kafka-ui.provectus.io/configuration/configuration-file
```

## Using Kafka Tools

All Kafka command-line tools are in the PATH:

```bash
# List topics
kafka-topics.sh --bootstrap-server localhost:9092 --list

# Create a topic
kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic test-topic \
  --partitions 3 --replication-factor 1

# Console producer
kafka-console-producer.sh --bootstrap-server localhost:9092 \
  --topic test-topic

# Console consumer
kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic test-topic --from-beginning

# Consumer groups
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list
```

**Note:** Update `localhost:9092` with your actual Kafka bootstrap servers (e.g., MSK cluster endpoints).

## Connecting to AWS MSK

To use this AMI with AWS MSK:

1. **Get MSK bootstrap servers:**

```bash
aws kafka get-bootstrap-brokers \
  --cluster-arn <your-msk-cluster-arn>
```

2. **Set up environment variables:**

```bash
# Use the environment files from this repo
cd /home/ec2-user
git clone https://github.com/your-repo/kafka-tutorials.git
cd kafka-tutorials

# Copy and configure MSK environment
cp infra/env-example.msk infra/env.msk
nano infra/env.msk  # Add your MSK bootstrap servers

# Source the environment
source infra/env.msk
```

3. **Test connectivity:**

```bash
bash/chapters/01-environment/check_connection.sh
```

## Customization

### Change Kafka Version

Edit `imagebuilder-component.yml`, in the `InstallKafka` step:

```yaml
- name: InstallKafka
  action: ExecuteBash
  inputs:
    commands:
      - echo "Installing Apache Kafka..."
      - cd /tmp
      # Change these lines to specify version:
      - KAFKA_VERSION="3.6.1"  # Specify exact version
      - SCALA_VERSION="2.13"
      - wget -q "https://downloads.apache.org/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"
      # ... rest of commands
```

### Add Additional Tools

Add new steps to the `build` phase in `imagebuilder-component.yml`:

```yaml
- name: InstallMyTool
  action: ExecuteBash
  inputs:
    commands:
      - echo "Installing my custom tool..."
      - yum install -y my-package
```

### Modify Kafka UI Configuration

Edit the `SetupKafkaUI` step to customize the docker-compose configuration:

```yaml
environment:
  - DYNAMIC_CONFIG_ENABLED=true
  - AUTH_TYPE=DISABLED  # or configure authentication
  - KAFKA_CLUSTERS_0_NAME=my-cluster
  - KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS=my-kafka:9092
```

## Troubleshooting

### Build Fails During Component Installation

1. Check CloudWatch Logs for the build instance
2. Check S3 bucket logs: `s3://{StackName}-components-{AccountId}/logs/`
3. Review the specific step that failed

Common issues:
- **Network connectivity**: Ensure subnet has internet access
- **Download failures**: Check if upstream sources are available
- **Permissions**: Verify IAM role has required permissions

### Kafka UI Not Starting

```bash
# Check service status
sudo systemctl status kafka-ui

# Check Docker Compose logs
cd /opt/kafka-ui
sudo docker-compose logs

# Manually start
sudo docker-compose up -d
```

### Kafka Tools Not in PATH

```bash
# Manually source the profile
source /etc/profile.d/kafka.sh

# Or add to your ~/.bashrc
echo 'source /etc/profile.d/kafka.sh' >> ~/.bashrc
```

## Costs

Estimated costs for Image Builder:

- **Build time**: ~30 minutes
- **Instance costs**: ~$0.02 per build (t3.medium for 30 min)
- **Storage**: $0.05/GB/month for AMIs (typically 8-10 GB)
- **S3 logs**: Negligible (<$0.01/month)

**Total estimated cost per build**: ~$0.05-0.10

## Security Best Practices

1. **Delete old AMIs**: Set up lifecycle policies to delete old AMIs
2. **Encrypt AMIs**: The template enables EBS encryption by default
3. **Restrict access**: Use IAM policies to control who can launch AMIs
4. **Update regularly**: Rebuild AMIs monthly to include security patches
5. **Scan for vulnerabilities**: Use Amazon Inspector to scan AMIs

## Cleanup

To delete all resources:

```bash
# Delete the CloudFormation stack
aws cloudformation delete-stack \
  --stack-name kafka-dev-image-builder

# Manually delete AMIs created by the pipeline
aws ec2 describe-images \
  --owners self \
  --filters "Name=name,Values=kafka-dev-*" \
  --query 'Images[*].ImageId' \
  --output text | xargs -n 1 aws ec2 deregister-image --image-id

# Delete snapshots
aws ec2 describe-snapshots \
  --owner-ids self \
  --filters "Name=description,Values=*kafka-dev*" \
  --query 'Snapshots[*].SnapshotId' \
  --output text | xargs -n 1 aws ec2 delete-snapshot --snapshot-id
```

## Support and Contribution

For issues or contributions, please refer to the main repository README.

## References

- [EC2 Image Builder Documentation](https://docs.aws.amazon.com/imagebuilder/)
- [Kafka Documentation](https://kafka.apache.org/documentation/)
- [Kafka UI Documentation](https://docs.kafka-ui.provectus.io/)
- [AWS MSK Documentation](https://docs.aws.amazon.com/msk/)
