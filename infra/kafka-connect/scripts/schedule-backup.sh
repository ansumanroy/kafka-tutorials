#!/usr/bin/env bash
set -euo pipefail

# Schedule backup using cron or EventBridge
# Usage: schedule-backup.sh <connect-rest-url> <s3-bucket> [cron|eventbridge] [schedule]

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# shellcheck source=../../bash/common/log.sh
source "$(dirname "$SCRIPT_DIR")/../../bash/common/log.sh"

CONNECT_REST_URL="${1:?Kafka Connect REST API URL required}"
S3_BUCKET="${2:?S3 bucket name required}"
SCHEDULER_TYPE="${3:-cron}"
SCHEDULE="${4:-0 2 * * *}"  # Default: Daily at 2 AM

BACKUP_SCRIPT="$SCRIPT_DIR/backup.sh"

if [[ ! -f "$BACKUP_SCRIPT" ]]; then
  error "Backup script not found: $BACKUP_SCRIPT"
  exit 1
fi

case "$SCHEDULER_TYPE" in
  cron)
    info "Setting up cron job for scheduled backups"
    info "Schedule: $SCHEDULE"
    
    # Create cron job entry
    CRON_ENTRY="$SCHEDULE $BACKUP_SCRIPT $CONNECT_REST_URL $S3_BUCKET >> /var/log/kafka-backup.log 2>&1"
    
    # Add to crontab
    (crontab -l 2>/dev/null | grep -v "$BACKUP_SCRIPT" || true; echo "$CRON_ENTRY") | crontab -
    
    info "✓ Cron job added"
    info "View with: crontab -l"
    info "Logs: /var/log/kafka-backup.log"
    ;;
    
  eventbridge)
    info "Setting up EventBridge rule for scheduled backups"
    info "Schedule: $SCHEDULE"
    
    # Convert cron to EventBridge rate or cron expression
    # EventBridge uses cron(5 fields) or rate(1 minute) format
    if [[ "$SCHEDULE" =~ ^rate\( ]]; then
      EB_SCHEDULE="$SCHEDULE"
    else
      # Convert standard cron to EventBridge cron
      EB_SCHEDULE="cron($SCHEDULE)"
    fi
    
    info "EventBridge schedule expression: $EB_SCHEDULE"
    info ""
    info "To create EventBridge rule manually:"
    info "1. Create a Lambda function that runs the backup script"
    info "2. Create an EventBridge rule with schedule: $EB_SCHEDULE"
    info "3. Add the Lambda function as a target"
    info ""
    info "Or use Terraform/CloudFormation to automate this"
    ;;
    
  *)
    error "Unknown scheduler type: $SCHEDULER_TYPE (use 'cron' or 'eventbridge')"
    exit 1
    ;;
esac

info "Backup scheduling configured"

