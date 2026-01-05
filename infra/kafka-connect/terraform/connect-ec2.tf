# Data source for MSK cluster to get bootstrap brokers
data "aws_msk_cluster" "main" {
  cluster_name = var.msk_cluster_name
}

# Security Group for Kafka Connect
resource "aws_security_group" "kafka_connect" {
  name        = "kafka-connect-sg"
  description = "Security group for Kafka Connect EC2 instance"
  vpc_id      = var.vpc_id

  # Inbound: SSH (if key pair is provided)
  dynamic "ingress" {
    for_each = var.kafka_connect_key_name != "" ? [1] : []
    content {
      from_port   = 22
      to_port     = 22
      protocol    = "tcp"
      cidr_blocks = ["0.0.0.0/0"]
      description = "SSH access"
    }
  }

  # Inbound: Kafka Connect REST API
  ingress {
    from_port   = var.kafka_connect_rest_port
    to_port     = var.kafka_connect_rest_port
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/8"]  # Adjust based on your VPC CIDR
    description = "Kafka Connect REST API"
  }

  # Outbound: All traffic (for S3, MSK, etc.)
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
    description = "All outbound traffic"
  }

  tags = merge(
    var.tags,
    {
      Name = "kafka-connect-sg"
    }
  )
}

# Data source for latest Amazon Linux 2 AMI
data "aws_ami" "amazon_linux_2" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["amzn2-ami-hvm-*-x86_64-gp2"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

# User data script to install Docker and set up Kafka Connect
locals {
  user_data = <<-EOF
#!/bin/bash
set -e

# Update system
yum update -y

# Install Docker
yum install -y docker
systemctl enable docker
systemctl start docker
usermod -aG docker ec2-user

# Install Docker Compose
mkdir -p /usr/local/lib/docker/cli-plugins
COMPOSE_VERSION=$(curl -s https://api.github.com/repos/docker/compose/releases/latest | grep 'tag_name' | cut -d'"' -f4)
curl -SL "https://github.com/docker/compose/releases/download/${COMPOSE_VERSION}/docker-compose-linux-x86_64" -o /usr/local/lib/docker/cli-plugins/docker-compose
chmod +x /usr/local/lib/docker/cli-plugins/docker-compose
ln -sf /usr/local/lib/docker/cli-plugins/docker-compose /usr/local/bin/docker-compose

# Install AWS CLI v2
cd /tmp
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip -q awscliv2.zip
./aws/install
rm -rf awscliv2.zip aws

# Install jq for JSON processing
yum install -y jq

# Create directories for Kafka Connect
mkdir -p /opt/kafka-connect/{config,plugins,scripts,connectors}
chown -R ec2-user:ec2-user /opt/kafka-connect

# Install CloudWatch agent
yum install -y amazon-cloudwatch-agent

# Start CloudWatch agent (basic config)
cat > /opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json <<'AGENTCONF'
{
  "logs": {
    "logs_collected": {
      "files": {
        "collect_list": [
          {
            "file_path": "/opt/kafka-connect/logs/*.log",
            "log_group_name": "/aws/kafka-connect/connect",
            "log_stream_name": "{instance_id}"
          }
        ]
      }
    }
  }
}
AGENTCONF
systemctl enable amazon-cloudwatch-agent
systemctl start amazon-cloudwatch-agent

# Note: Actual Kafka Connect docker-compose and configs will be deployed separately
echo "Kafka Connect EC2 instance initialized"
EOF
}

# EC2 Instance for Kafka Connect
resource "aws_instance" "kafka_connect" {
  ami           = data.aws_ami.amazon_linux_2.id
  instance_type = var.instance_type
  subnet_id     = var.subnet_ids[0]  # Use first subnet

  vpc_security_group_ids = concat(
    [aws_security_group.kafka_connect.id],
    var.security_group_ids
  )

  iam_instance_profile = aws_iam_instance_profile.kafka_connect.name

  key_name = var.kafka_connect_key_name != "" ? var.kafka_connect_key_name : null

  user_data = local.user_data

  root_block_device {
    volume_type = "gp3"
    volume_size = 50
    encrypted   = true
  }

  tags = merge(
    var.tags,
    {
      Name = "kafka-connect"
    }
  )
}

