# Kafka Connect Terraform Configuration

This Terraform configuration deploys an EC2 instance running Kafka Connect for backing up MSK topics to S3.

## Prerequisites

1. An existing MSK cluster
2. An existing S3 bucket for backups
3. VPC and subnet IDs where the instance will be deployed
4. Terraform >= 1.0
5. AWS credentials configured

## Setup

1. Copy the example variables file:
   ```bash
   cp terraform.tfvars.example terraform.tfvars
   ```

2. Edit `terraform.tfvars` with your values:
   - MSK cluster name and ARN
   - VPC and subnet IDs
   - S3 bucket name
   - Other configuration as needed

3. Initialize Terraform:
   ```bash
   terraform init
   ```

4. Review the plan:
   ```bash
   terraform plan
   ```

5. Apply the configuration:
   ```bash
   terraform apply
   ```

## Resources Created

- EC2 instance for Kafka Connect
- IAM role and instance profile with permissions for:
  - S3 access (read/write to backup bucket)
  - MSK cluster access (if using IAM authentication)
  - CloudWatch Logs access
- Security group for Kafka Connect

## Outputs

After deployment, Terraform will output:
- Instance ID
- Private IP address
- Kafka Connect REST API URL
- MSK bootstrap servers
- IAM role ARN

## Post-Deployment

After the EC2 instance is created, you need to:

1. SSH into the instance (if key pair was provided)
2. Deploy the Kafka Connect docker-compose configuration
3. Copy configuration files and scripts to `/opt/kafka-connect/`
4. Start Kafka Connect service

See the main README.md for detailed setup instructions.

