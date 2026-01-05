import os
import time
import json
import random
from datetime import datetime, timezone
from typing import Dict

import boto3
import urllib.request
import urllib.error


STAGING_BUCKET = os.environ["STAGING_BUCKET"]
SCANNED_BUCKET = os.environ["SCANNED_BUCKET"]
INFECTED_BUCKET = os.environ["INFECTED_BUCKET"]

SCAN_SLEEP_SECONDS = int(os.environ.get("SCAN_SLEEP_SECONDS", "120"))
DELETE_FROM_STAGING = os.environ.get("DELETE_FROM_STAGING", "true").lower() == "true"
CLEAN_PROBABILITY = float(os.environ.get("CLEAN_PROBABILITY", "0.9"))

SF_ENDPOINT_URL = os.environ.get("SALESFORCE_ENDPOINT_URL", "")
SF_JWT_SECRET_ARN = os.environ.get("SALESFORCE_JWT_SECRET_ARN", "")
SF_TIMEOUT_SECONDS = int(os.environ.get("SALESFORCE_TIMEOUT_SECONDS", "5"))

s3 = boto3.client("s3")
secrets_client = boto3.client("secretsmanager")


def tag_object(bucket: str, key: str, tags: Dict[str, str]) -> None:
    """Set object tags (overwrite existing)."""
    tagset = [{"Key": k, "Value": str(v)} for k, v in tags.items()]
    s3.put_object_tagging(
        Bucket=bucket,
        Key=key,
        Tagging={"TagSet": tagset},
    )


def copy_object(src_bucket: str, src_key: str, dest_bucket: str, dest_key: str) -> None:
    copy_source = {"Bucket": src_bucket, "Key": src_key}
    s3.copy_object(
        Bucket=dest_bucket,
        Key=dest_key,
        CopySource=copy_source,
    )


def get_salesforce_jwt() -> str:
    """Fetch JWT string from Secrets Manager."""
    if not SF_JWT_SECRET_ARN:
        raise RuntimeError("SALESFORCE_JWT_SECRET_ARN not configured")
    resp = secrets_client.get_secret_value(SecretId=SF_JWT_SECRET_ARN)
    secret = resp.get("SecretString")
    if not secret:
        raise RuntimeError("SecretString is empty for Salesforce JWT")
    return secret.strip()


def notify_salesforce(bucket: str, key: str, status: str, timestamp: str) -> None:
    """Send a POST request to Salesforce with object status."""
    if not SF_ENDPOINT_URL:
        print("SALESFORCE_ENDPOINT_URL not set; skipping Salesforce call")
        return

    try:
        jwt = get_salesforce_jwt()
    except Exception as e:
        print(f"Failed to fetch Salesforce JWT: {e}")
        return

    payload = json.dumps(
        {
            "bucket": bucket,
            "key": key,
            "status": status,
            "timestamp": timestamp,
        }
    ).encode("utf-8")

    req = urllib.request.Request(
        SF_ENDPOINT_URL,
        data=payload,
        headers={
            "Authorization": f"Bearer {jwt}",
            "Content-Type": "application/json",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=SF_TIMEOUT_SECONDS) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            print(f"Salesforce response: {resp.status} {body}")
    except urllib.error.HTTPError as e:
        try:
            err_body = e.read().decode("utf-8", errors="replace")
        except Exception:
            err_body = "<no body>"
        print(f"Salesforce HTTPError: {e.code} {err_body}")
    except urllib.error.URLError as e:
        print(f"Salesforce URLError: {e}")
    except Exception as e:
        print(f"Salesforce call failed: {e}")


def lambda_handler(event, context):
    # Event is standard S3 put event
    records = event.get("Records", [])
    for record in records:
        try:
            bucket = record["s3"]["bucket"]["name"]
            key = record["s3"]["object"]["key"]

            # Only handle events for the configured staging bucket
            if bucket != STAGING_BUCKET:
                print(f"Skipping object from unexpected bucket: {bucket}")
                continue

            print(f"Received object: s3://{bucket}/{key}")

            # Tag as scan in progress
            tag_object(bucket, key, {"scan-status": "in-progress"})

            # Mimic scan delay
            print(f"Sleeping for {SCAN_SLEEP_SECONDS} seconds to mimic scan")
            time.sleep(SCAN_SLEEP_SECONDS)

            # Decide clean vs infected
            is_clean = random.random() < CLEAN_PROBABILITY
            now_iso = datetime.now(timezone.utc).isoformat()

            if is_clean:
                dest_bucket = SCANNED_BUCKET
                status = "scanned"
            else:
                dest_bucket = INFECTED_BUCKET
                status = "infected"

            print(f"Scan result for {key}: {status}, copying to {dest_bucket}")

            # Copy to destination bucket with same key
            copy_object(STAGING_BUCKET, key, dest_bucket, key)

            # Tag destination with final status and timestamp
            tag_object(
                dest_bucket,
                key,
                {
                    "scan-status": status,
                    "scan-timestamp": now_iso,
                },
            )

            # Notify Salesforce (best-effort)
            notify_salesforce(dest_bucket, key, status, now_iso)

            # Optionally delete from staging
            if DELETE_FROM_STAGING:
                print(f"Deleting original from staging: s3://{STAGING_BUCKET}/{key}")
                s3.delete_object(Bucket=STAGING_BUCKET, Key=key)

        except Exception as e:
            # On error, best-effort tag staging object as error
            print(f"Error processing record: {e}")
            try:
                if "s3" in record:
                    b = record["s3"]["bucket"]["name"]
                    k = record["s3"]["object"]["key"]
                    if b == STAGING_BUCKET:
                        tag_object(b, k, {"scan-status": "error"})
            except Exception as tag_err:
                print(f"Failed to tag object on error: {tag_err}")

    return {"statusCode": 200, "body": json.dumps({"processed": len(records)})}


