## Virus Scan Mimic – Terraform Module

This module creates a **placeholder virus scanning pipeline** using a single Lambda function that:

- Mimics scan time and moves objects between three S3 buckets, and
- Optionally invokes a **Salesforce API** after each scan using a JWT token from AWS Secrets Manager.

- **Staging bucket**: `sf-bucket-staging` (upload here)
- **Scanned bucket**: `sf-bucket-scanned` (clean objects end up here)
- **Infected bucket**: `sf-bucket-infected` (failed/\"infected\" objects end up here)

It is intentionally simple so you can plug in a real scanner (ClamAV, vendor API, etc.) later without changing the overall flow.

---

## Flow

1. An object is uploaded to the **staging** bucket.
2. S3 emits an `ObjectCreated:*` event to the **scan-mimic Lambda**.
3. Lambda:
   - Tags the staging object: `scan-status=in-progress`.
   - Sleeps for `scan_sleep_seconds` (to mimic scan time).
   - Randomly marks the object as **clean** or **infected** using `clean_probability`.
   - Copies the object to either:
     - `sf-bucket-scanned` with tags:
       - `scan-status=scanned`
       - `scan-timestamp=<UTC ISO8601>`
     - `sf-bucket-infected` with tags:
       - `scan-status=infected`
       - `scan-timestamp=<UTC ISO8601>`
   - Optionally deletes the original from staging.

This mirrors a real scanning pipeline but without any actual AV engine.

---

## Files

- `main.tf`
  - Providers: `aws`, `archive`.
  - `aws_cloudwatch_log_group` for the Lambda.
  - IAM role + policy for Lambda (S3 + CloudWatch Logs).
  - `archive_file.scan_mimic_zip` to package the Lambda.
  - `aws_lambda_function.scan_mimic` (Python 3.11).
  - `aws_lambda_permission.allow_s3_invoke` so S3 can invoke the Lambda.
  - `aws_s3_bucket_notification.staging_notify` to wire `sf-bucket-staging` → Lambda.

- `variables.tf`
  - **Core:**
    - `region`
    - `staging_bucket_name` (default `sf-bucket-staging`)
    - `scanned_bucket_name` (default `sf-bucket-scanned`)
    - `infected_bucket_name` (default `sf-bucket-infected`)
  - **Lambda runtime:**
    - `lambda_timeout_seconds` (default `300`)
    - `lambda_memory_mb` (default `512`)
    - `scan_sleep_seconds` (default `120`)
    - `delete_from_staging` (default `true`)
    - `clean_probability` (default `0.9`)
  - **Salesforce callback (optional):**
    - `salesforce_endpoint_url` – Salesforce REST endpoint URL (default `""` = disabled)
    - `salesforce_jwt_secret_arn` – ARN of Secrets Manager secret storing JWT (default `""`)
    - `salesforce_timeout_seconds` – HTTP timeout for Salesforce calls (default `5`)

- `outputs.tf`
  - `scan_mimic_lambda_arn`
  - `scan_mimic_log_group`

- `lambda/scan_mimic.py`
  - Lambda handler with full placeholder logic:
    - Tag `scan-status=in-progress` on staging object.
    - Sleep for `SCAN_SLEEP_SECONDS`.
    - Randomly choose clean vs infected.
    - Copy to scanned or infected bucket.
    - Tag destination with final status + timestamp.
    - Optionally delete original from staging.
    - Tag staging object with `scan-status=error` on failure.
    - **After each scan (clean or infected),** best-effort call to Salesforce:
      - Fetch JWT from Secrets Manager using `salesforce_jwt_secret_arn`.
      - POST JSON payload `{ bucket, key, status, timestamp }` to `salesforce_endpoint_url` with `Authorization: Bearer <jwt>`.

---

## Usage

From the root of this repo:

```bash
cd infra/virusmimic

terraform init
terraform apply \
  -var="region=us-east-1" \
  -var="staging_bucket_name=sf-bucket-staging" \
  -var="scanned_bucket_name=sf-bucket-scanned" \
  -var="infected_bucket_name=sf-bucket-infected"
```

You can also set these via `terraform.tfvars`:

```hcl
region              = "us-east-1"
staging_bucket_name = "sf-bucket-staging"
scanned_bucket_name = "sf-bucket-scanned"
infected_bucket_name = "sf-bucket-infected"
scan_sleep_seconds  = 180
clean_probability   = 0.95
delete_from_staging = true
```

After `apply`:

1. Upload a test file to `sf-bucket-staging`.
2. Watch CloudWatch Logs for `/aws/lambda/scan-mimic-lambda`.
3. Confirm the object appears in either:
   - `sf-bucket-scanned` with `scan-status=scanned`, or
   - `sf-bucket-infected` with `scan-status=infected`.

---

## Migration to a Real Scanner

To plug in a real scanner later:

- Keep the **Terraform wiring** and environment variables as-is.
- Replace the internals of `scan_mimic.py`:
  - Swap out `time.sleep` + random decision with:
    - A call to a real scanning service/API, or
    - An SQS message to a dedicated scanner worker and polling for result.
- Keep the tagging and copy/move semantics identical so downstream systems don’t change.

This module is meant to be a safe, observable placeholder while you evaluate GuardDuty, ClamAV, or vendor-based scanning solutions.

