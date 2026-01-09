#!/usr/bin/env bash
# Script to get EC2 instance public IP from metadata service
# Usage: ./get-ec2-public-ip.sh

set -euo pipefail

# Get public IP from EC2 metadata service
PUBLIC_IP=$(curl -s http://169.254.169.254/latest/meta-data/public-ipv4 2>/dev/null || echo "")

if [[ -z "$PUBLIC_IP" ]]; then
  echo "Error: Could not retrieve EC2 public IP from metadata service." >&2
  echo "This script must be run on an EC2 instance." >&2
  exit 1
fi

echo "$PUBLIC_IP"

