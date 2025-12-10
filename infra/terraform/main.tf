terraform {
  required_version = ">= 1.0"
  
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# Data source for current AWS account
data "aws_caller_identity" "current" {}

# Data source for current AWS region
data "aws_region" "current" {}

# S3 Bucket for Image Builder logs and components
resource "aws_s3_bucket" "component_bucket" {
  bucket = "${var.stack_name}-components-${data.aws_caller_identity.current.account_id}"

  tags = {
    Name        = "${var.stack_name}-components"
    Environment = "Development"
  }
}

resource "aws_s3_bucket_versioning" "component_bucket" {
  bucket = aws_s3_bucket.component_bucket.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_public_access_block" "component_bucket" {
  bucket = aws_s3_bucket.component_bucket.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "component_bucket" {
  bucket = aws_s3_bucket.component_bucket.id

  rule {
    id     = "delete-old-versions"
    status = "Enabled"

    noncurrent_version_expiration {
      noncurrent_days = 30
    }
  }
}

# SNS Topic for build notifications (optional)
resource "aws_sns_topic" "notifications" {
  count = var.notification_email != "" ? 1 : 0

  name         = "${var.stack_name}-notifications"
  display_name = "Image Builder Build Notifications"

  tags = {
    Name = "${var.stack_name}-notifications"
  }
}

resource "aws_sns_topic_subscription" "notifications" {
  count = var.notification_email != "" ? 1 : 0

  topic_arn = aws_sns_topic.notifications[0].arn
  protocol  = "email"
  endpoint  = var.notification_email
}

# IAM Role for Image Builder
resource "aws_iam_role" "image_builder" {
  name = "${var.stack_name}-ImageBuilderRole"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  managed_policy_arns = [
    "arn:aws:iam::aws:policy/EC2InstanceProfileForImageBuilder",
    "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore",
    "arn:aws:iam::aws:policy/CloudWatchAgentServerPolicy"
  ]

  tags = {
    Name = "${var.stack_name}-ImageBuilderRole"
  }
}

# IAM Policy for S3 access
resource "aws_iam_role_policy" "s3_access" {
  name = "ImageBuilderS3Access"
  role = aws_iam_role.image_builder.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket"
        ]
        Resource = [
          "arn:aws:s3:::ec2imagebuilder*",
          "arn:aws:s3:::ec2imagebuilder*/*"
        ]
      }
    ]
  })
}

# IAM Policy for CloudWatch Logs
resource "aws_iam_role_policy" "logs_access" {
  name = "ImageBuilderLogsAccess"
  role = aws_iam_role.image_builder.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/imagebuilder/*"
      }
    ]
  })
}

# IAM Instance Profile
resource "aws_iam_instance_profile" "image_builder" {
  name = "${var.stack_name}-InstanceProfile"
  role = aws_iam_role.image_builder.name

  tags = {
    Name = "${var.stack_name}-InstanceProfile"
  }
}

# Image Builder Component
resource "aws_imagebuilder_component" "kafka_dev" {
  name        = "${var.stack_name}-KafkaDevComponent"
  description = "Install Kafka tools, Python3, AWS CLI v2, Docker, and Kafka UI"
  platform    = "Linux"
  version     = var.component_version

  data = yamlencode({
    name          = "KafkaDevEnvironment"
    description   = "Install Kafka tools, Python3, AWS CLI v2, Docker, and Kafka UI on Amazon Linux 2"
    schemaVersion = 1.0

    phases = [
      {
        name = "build"
        steps = [
          {
            name   = "UpdateSystem"
            action = "ExecuteBash"
            inputs = {
              commands = [
                "echo 'Updating system packages...'",
                "yum update -y",
                "yum install -y wget tar gzip unzip"
              ]
            }
          },
          {
            name   = "InstallPython3"
            action = "ExecuteBash"
            inputs = {
              commands = [
                "echo 'Installing Python 3...'",
                "yum install -y python3 python3-pip",
                "python3 --version"
              ]
            }
          },
          {
            name   = "InstallAWSCLIv2"
            action = "ExecuteBash"
            inputs = {
              commands = [
                "echo 'Installing AWS CLI v2...'",
                "cd /tmp",
                "curl \"https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip\" -o \"awscliv2.zip\"",
                "unzip -q awscliv2.zip",
                "./aws/install",
                "/usr/local/bin/aws --version",
                "rm -rf awscliv2.zip aws"
              ]
            }
          },
          {
            name   = "InstallJava"
            action = "ExecuteBash"
            inputs = {
              commands = [
                "echo 'Installing Java 11...'",
                "yum install -y java-11-amazon-corretto-headless",
                "java -version"
              ]
            }
          },
          {
            name   = "InstallDocker"
            action = "ExecuteBash"
            inputs = {
              commands = [
                "echo 'Installing Docker...'",
                "yum install -y docker",
                "systemctl enable docker",
                "systemctl start docker",
                "usermod -aG docker ec2-user",
                "docker --version"
              ]
            }
          },
          {
            name   = "InstallDockerCompose"
            action = "ExecuteBash"
            inputs = {
              commands = [
                "echo 'Installing Docker Compose v2...'",
                "mkdir -p /usr/local/lib/docker/cli-plugins",
                "COMPOSE_VERSION=$(curl -s https://api.github.com/repos/docker/compose/releases/latest | grep 'tag_name' | cut -d\\\" -f4)",
                "curl -SL \"https://github.com/docker/compose/releases/download/$${COMPOSE_VERSION}/docker-compose-linux-x86_64\" -o /usr/local/lib/docker/cli-plugins/docker-compose",
                "chmod +x /usr/local/lib/docker/cli-plugins/docker-compose",
                "ln -sf /usr/local/lib/docker/cli-plugins/docker-compose /usr/local/bin/docker-compose",
                "docker-compose --version"
              ]
            }
          },
          {
            name   = "InstallKafka"
            action = "ExecuteBash"
            inputs = {
              commands = [
                "echo 'Installing Apache Kafka...'",
                "cd /tmp",
                "KAFKA_VERSION=$(curl -s https://downloads.apache.org/kafka/ | grep -oP 'kafka_[0-9]+\\.[0-9]+-[0-9]+\\.[0-9]+\\.[0-9]+' | sort -V | tail -1 | cut -d'_' -f2)",
                "SCALA_VERSION=\"2.13\"",
                "wget -q \"https://downloads.apache.org/kafka/$${KAFKA_VERSION}/kafka_$${SCALA_VERSION}-$${KAFKA_VERSION}.tgz\"",
                "mkdir -p /opt/kafka",
                "tar -xzf \"kafka_$${SCALA_VERSION}-$${KAFKA_VERSION}.tgz\" -C /opt/kafka",
                "mv /opt/kafka/kafka_$${SCALA_VERSION}-$${KAFKA_VERSION} /opt/kafka/kafka_$${KAFKA_VERSION}",
                "ln -sf /opt/kafka/kafka_$${KAFKA_VERSION} /opt/kafka/current",
                "rm -f \"kafka_$${SCALA_VERSION}-$${KAFKA_VERSION}.tgz\"",
                "chown -R ec2-user:ec2-user /opt/kafka"
              ]
            }
          },
          {
            name   = "ConfigureKafkaPath"
            action = "ExecuteBash"
            inputs = {
              commands = [
                "echo 'Configuring Kafka PATH...'",
                "cat > /etc/profile.d/kafka.sh << 'EOF'\nexport KAFKA_HOME=/opt/kafka/current\nexport PATH=$PATH:$KAFKA_HOME/bin\nEOF",
                "chmod +x /etc/profile.d/kafka.sh"
              ]
            }
          },
          {
            name   = "SetupKafkaUI"
            action = "ExecuteBash"
            inputs = {
              commands = [
                "echo 'Setting up Kafka UI...'",
                "mkdir -p /opt/kafka-ui",
                "cat > /opt/kafka-ui/docker-compose.yml << 'EOF'\nversion: '3.8'\nservices:\n  kafka-ui:\n    image: provectuslabs/kafka-ui:latest\n    container_name: kafka-ui\n    ports:\n      - \"8080:8080\"\n    environment:\n      - DYNAMIC_CONFIG_ENABLED=true\n    restart: unless-stopped\nEOF",
                "chown -R ec2-user:ec2-user /opt/kafka-ui"
              ]
            }
          },
          {
            name   = "CreateKafkaUIService"
            action = "ExecuteBash"
            inputs = {
              commands = [
                "echo 'Creating Kafka UI systemd service...'",
                "cat > /etc/systemd/system/kafka-ui.service << 'EOF'\n[Unit]\nDescription=Kafka UI Docker Container\nRequires=docker.service\nAfter=docker.service\n\n[Service]\nType=oneshot\nRemainAfterExit=yes\nWorkingDirectory=/opt/kafka-ui\nExecStart=/usr/local/bin/docker-compose up -d\nExecStop=/usr/local/bin/docker-compose down\nUser=root\n\n[Install]\nWantedBy=multi-user.target\nEOF",
                "systemctl daemon-reload",
                "systemctl enable kafka-ui.service"
              ]
            }
          }
        ]
      },
      {
        name = "test"
        steps = [
          {
            name   = "TestInstallations"
            action = "ExecuteBash"
            inputs = {
              commands = [
                "python3 --version",
                "/usr/local/bin/aws --version",
                "docker --version",
                "docker-compose --version",
                "java -version",
                "source /etc/profile.d/kafka.sh && /opt/kafka/current/bin/kafka-topics.sh --version"
              ]
            }
          }
        ]
      },
      {
        name = "validate"
        steps = [
          {
            name   = "ValidateAll"
            action = "ExecuteBash"
            inputs = {
              commands = [
                "test -x /usr/bin/python3 && echo '✓ Python3'",
                "test -x /usr/local/bin/aws && echo '✓ AWS CLI'",
                "test -x /usr/bin/docker && echo '✓ Docker'",
                "test -x /opt/kafka/current/bin/kafka-topics.sh && echo '✓ Kafka'",
                "systemctl is-enabled docker && echo '✓ Docker enabled'",
                "systemctl is-enabled kafka-ui && echo '✓ Kafka UI enabled'"
              ]
            }
          }
        ]
      }
    ]
  })

  supported_os_versions = ["Amazon Linux 2"]

  tags = {
    Name        = "${var.stack_name}-KafkaDevComponent"
    Environment = "Development"
  }
}

# Image Recipe
resource "aws_imagebuilder_image_recipe" "kafka_dev" {
  name         = "${var.stack_name}-Recipe"
  description  = "Kafka Development Environment Recipe"
  parent_image = var.base_image_id
  version      = var.component_version

  component {
    component_arn = aws_imagebuilder_component.kafka_dev.arn
  }

  block_device_mapping {
    device_name = "/dev/xvda"

    ebs {
      volume_size           = 30
      volume_type           = "gp3"
      delete_on_termination = true
      encrypted             = true
    }
  }

  tags = {
    Name        = "${var.stack_name}-Recipe"
    Environment = "Development"
  }
}

# Infrastructure Configuration
resource "aws_imagebuilder_infrastructure_configuration" "kafka_dev" {
  name                          = "${var.stack_name}-Infrastructure"
  description                   = "Infrastructure configuration for Kafka Dev Image Builder"
  instance_profile_name         = aws_iam_instance_profile.image_builder.name
  instance_types                = [var.instance_type]
  subnet_id                     = var.subnet_id
  security_group_ids            = [var.security_group_id]
  terminate_instance_on_failure = true
  sns_topic_arn                 = var.notification_email != "" ? aws_sns_topic.notifications[0].arn : null

  logging {
    s3_logs {
      s3_bucket_name = aws_s3_bucket.component_bucket.id
      s3_key_prefix  = "logs/"
    }
  }

  tags = {
    Name = "${var.stack_name}-Infrastructure"
  }
}

# Distribution Configuration
resource "aws_imagebuilder_distribution_configuration" "kafka_dev" {
  name        = "${var.stack_name}-Distribution"
  description = "Distribution configuration for Kafka Dev AMI"

  distribution {
    region = data.aws_region.current.name

    ami_distribution_configuration {
      name        = "kafka-dev-{{ imagebuilder:buildDate }}"
      description = "Kafka Development Environment AMI"

      ami_tags = {
        Name         = "${var.stack_name}-AMI"
        Environment  = "Development"
        BuildDate    = "{{ imagebuilder:buildDate }}"
        SourceRecipe = "${var.stack_name}-Recipe"
      }

      launch_permission {
        user_ids = var.ami_user_ids
      }
    }
  }

  # Additional region distributions
  dynamic "distribution" {
    for_each = var.target_regions
    content {
      region = distribution.value

      ami_distribution_configuration {
        name        = "kafka-dev-{{ imagebuilder:buildDate }}"
        description = "Kafka Development Environment AMI"

        ami_tags = {
          Name         = "${var.stack_name}-AMI"
          Environment  = "Development"
          BuildDate    = "{{ imagebuilder:buildDate }}"
          SourceRecipe = "${var.stack_name}-Recipe"
        }

        launch_permission {
          user_ids = var.ami_user_ids
        }
      }
    }
  }

  tags = {
    Name = "${var.stack_name}-Distribution"
  }
}

# Image Pipeline
resource "aws_imagebuilder_image_pipeline" "kafka_dev" {
  name                             = "${var.stack_name}-Pipeline"
  description                      = "Automated pipeline for building Kafka Dev AMIs"
  image_recipe_arn                 = aws_imagebuilder_image_recipe.kafka_dev.arn
  infrastructure_configuration_arn = aws_imagebuilder_infrastructure_configuration.kafka_dev.arn
  distribution_configuration_arn   = aws_imagebuilder_distribution_configuration.kafka_dev.arn
  status                           = "ENABLED"
  enhanced_image_metadata_enabled  = true

  tags = {
    Name        = "${var.stack_name}-Pipeline"
    Environment = "Development"
  }
}
