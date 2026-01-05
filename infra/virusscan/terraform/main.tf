terraform {
  required_version = ">= 1.3"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.0"
    }
  }
}

provider "aws" {
  region = var.region
}

locals {
  name_prefix = var.name_prefix
}

# ------------------------------
# SQS queue for scan requests
# ------------------------------
resource "aws_sqs_queue" "scan_queue" {
  name                       = "${local.name_prefix}-scan-queue"
  visibility_timeout_seconds = 900
  message_retention_seconds  = 86400
  receive_wait_time_seconds  = 10
}

# ------------------------------
# IAM role / instance profile
# ------------------------------
data "aws_iam_policy_document" "instance_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "scanner_role" {
  name               = "${local.name_prefix}-scanner-role"
  assume_role_policy = data.aws_iam_policy_document.instance_assume.json
}

data "aws_iam_policy_document" "scanner_policy" {
  statement {
    sid     = "S3ReadScanTargets"
    effect  = "Allow"
    actions = ["s3:GetObject", "s3:GetObjectTagging", "s3:PutObjectTagging"]
    resources = [
      for arn in var.scan_bucket_arns : "${arn}/*"
    ]
  }

  statement {
    sid     = "SQSConsume"
    effect  = "Allow"
    actions = ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes", "sqs:ChangeMessageVisibility"]
    resources = [aws_sqs_queue.scan_queue.arn]
  }

  # Optional: allow writing results to CloudWatch Logs via the agent
  statement {
    sid     = "CWLogs"
    effect  = "Allow"
    actions = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["arn:aws:logs:${var.region}:${data.aws_caller_identity.current.account_id}:*"]
  }
}

resource "aws_iam_policy" "scanner_policy" {
  name   = "${local.name_prefix}-scanner-policy"
  policy = data.aws_iam_policy_document.scanner_policy.json
}

resource "aws_iam_role_policy_attachment" "attach" {
  role       = aws_iam_role.scanner_role.name
  policy_arn = aws_iam_policy.scanner_policy.arn
}

resource "aws_iam_instance_profile" "scanner_profile" {
  name = "${local.name_prefix}-scanner-profile"
  role = aws_iam_role.scanner_role.name
}

data "aws_caller_identity" "current" {}

# ------------------------------
# Security group
# ------------------------------
resource "aws_security_group" "scanner_sg" {
  name        = "${local.name_prefix}-scanner-sg"
  description = "Allow outbound for S3/SQS/Clam updates"
  vpc_id      = var.vpc_id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# ------------------------------
# Launch template
# ------------------------------
data "aws_ami" "al2023" {
  owners      = ["amazon"]
  most_recent = true

  filter {
    name   = "name"
    values = ["al2023-ami-minimal-*x86_64"]
  }
}

locals {
  user_data = <<-EOF
    #!/bin/bash
    set -xe

    yum update -y
    yum install -y clamav clamav-update python3 python3-pip awscli

    # Update ClamAV DB (best-effort; can be slow)
    freshclam || true

    # Configure clamd
    cat >/etc/clamd.d/scan.conf <<'CONF'
LogSyslog yes
TCPSocket 3310
TCPAddr 127.0.0.1
Foreground yes
User root
# Allow streaming large files (adjust if you expect >6GB)
StreamMaxLength 6000000000
MaxFileSize 0
MaxScanSize 0
CONF

    systemctl enable clamd@scan
    systemctl start clamd@scan

    mkdir -p /opt/virusscan
    cat >/opt/virusscan/scan.py <<'PYCODE'
import boto3, socket, struct, os, json, time

SQS_URL = os.environ.get("SQS_URL")
REGION = os.environ.get("AWS_REGION", "us-east-1")
CHUNK = 1024 * 1024  # 1MB

def scan_s3_object(bucket, key, clamd_host="127.0.0.1", clamd_port=3310):
    s3 = boto3.client("s3", region_name=REGION)
    resp = s3.get_object(Bucket=bucket, Key=key)
    body = resp["Body"]

    with socket.create_connection((clamd_host, clamd_port), timeout=60) as s:
        s.sendall(b"zINSTREAM\\0")
        for chunk in iter(lambda: body.read(CHUNK), b""):
            s.sendall(struct.pack("!I", len(chunk)))
            s.sendall(chunk)
        s.sendall(struct.pack("!I", 0))
        result = s.recv(4096).decode("utf-8", errors="replace")
        return result.strip()

def tag_result(bucket, key, result):
    s3 = boto3.client("s3", region_name=REGION)
    status = "clean"
    sig = ""
    if "FOUND" in result:
        status = "infected"
        sig = result.split("FOUND")[0].split(":")[-1].strip()
    elif "ERROR" in result:
        status = "error"
        sig = result
    tags = [{"Key": "scan-status", "Value": status}]
    if sig:
        tags.append({"Key": "scan-signature", "Value": sig[:256]})
    s3.put_object_tagging(Bucket=bucket, Key=key, Tagging={"TagSet": tags})

def process_message(msg):
    body = json.loads(msg["Body"])
    # If using S3 event via SNS, unwrap accordingly. Here assume direct S3 event in body.
    recs = body.get("Records", [])
    for rec in recs:
        s3info = rec.get("s3", {})
        bucket = s3info.get("bucket", {}).get("name")
        key = s3info.get("object", {}).get("key")
        if bucket and key:
            res = scan_s3_object(bucket, key)
            tag_result(bucket, key, res)
            print(f"Scanned s3://{bucket}/{key} => {res}")

def main():
    sqs = boto3.client("sqs", region_name=REGION)
    while True:
        resp = sqs.receive_message(QueueUrl=SQS_URL, MaxNumberOfMessages=5, WaitTimeSeconds=10)
        msgs = resp.get("Messages", [])
        if not msgs:
            time.sleep(2)
            continue
        for m in msgs:
            try:
                process_message(m)
            except Exception as e:
                print(f"Error processing message: {e}")
            # Always delete to avoid reprocessing; consider DLQ in production
            sqs.delete_message(QueueUrl=SQS_URL, ReceiptHandle=m["ReceiptHandle"])

if __name__ == "__main__":
    main()
PYCODE

    chmod +x /opt/virusscan/scan.py
    cat >/etc/systemd/system/virusscan.service <<'UNIT'
[Unit]
Description=Virus scan worker
After=network.target clamd@scan.service
Requires=clamd@scan.service

[Service]
Environment="SQS_URL=${sqs_url}"
ExecStart=/usr/bin/python3 /opt/virusscan/scan.py
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
UNIT

    systemctl daemon-reload
    systemctl enable virusscan
    systemctl start virusscan
  EOF
}

resource "aws_launch_template" "scanner_lt" {
  name_prefix   = "${local.name_prefix}-scanner-"
  image_id      = data.aws_ami.al2023.id
  instance_type = var.instance_type
  iam_instance_profile {
    name = aws_iam_instance_profile.scanner_profile.name
  }
  network_interfaces {
    security_groups             = [aws_security_group.scanner_sg.id]
    associate_public_ip_address = false
  }
  user_data = base64encode(
    replace(
      local.user_data,
      "${sqs_url}",
      aws_sqs_queue.scan_queue.url
    )
  )
  lifecycle {
    create_before_destroy = true
  }
}

# ------------------------------
# Auto Scaling Group
# ------------------------------
resource "aws_autoscaling_group" "scanner_asg" {
  name                      = "${local.name_prefix}-scanner-asg"
  desired_capacity          = var.asg_desired_capacity
  min_size                  = var.asg_min_size
  max_size                  = var.asg_max_size
  vpc_zone_identifier       = var.subnet_ids
  capacity_rebalance        = true
  health_check_grace_period = 120
  health_check_type         = "EC2"

  launch_template {
    id      = aws_launch_template.scanner_lt.id
    version = "$Latest"
  }

  tag {
    key                 = "Name"
    value               = "${local.name_prefix}-scanner"
    propagate_at_launch = true
  }
}

# ------------------------------
# Outputs
# ------------------------------
output "scan_queue_url" {
  value = aws_sqs_queue.scan_queue.url
}

output "autoscaling_group_name" {
  value = aws_autoscaling_group.scanner_asg.name
}

output "security_group_id" {
  value = aws_security_group.scanner_sg.id
}
