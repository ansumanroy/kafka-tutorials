# Terraform Configuration for EC2 Image Builder

This directory contains Terraform configurations for creating an EC2 Image Builder pipeline that builds Amazon Linux 2 AMIs with Kafka development tools.

## Prerequisites

1. **Terraform** - Version 1.0 or higher
2. **AWS CLI** - Configured with appropriate credentials
3. **AWS Account** with permissions to create:
   - EC2 Image Builder resources
   - IAM roles and policies
   - S3 buckets
   - SNS topics (if using notifications)

## Files

- `main.tf` - Main Terraform configuration with all AWS resources
- `variables.tf` - Variable definitions
- `outputs.tf` - Output definitions
- `terraform.tfvars.example` - Example variables file
- `README.md` - This file

## Quick Start

### 1. Configure Variables

Copy the example variables file:

```bash
cd infra/terraform
cp terraform.tfvars.example terraform.tfvars
```

Edit `terraform.tfvars` with your values:

```hcl
aws_region        = "us-east-1"
vpc_id            = "vpc-xxxxx"
subnet_id         = "subnet-xxxxx"
security_group_id = "sg-xxxxx"
notification_email = "your-email@example.com"  # Optional
```

### 2. Initialize Terraform

```bash
terraform init
```

### 3. Review the Plan

```bash
terraform plan
```

This will show you all resources that will be created.

### 4. Apply the Configuration

```bash
terraform apply
```

Type `yes` when prompted to create the resources.

### 5. Start a Build

After successful deployment, start an image build:

```bash
# Get the pipeline ARN from outputs
terraform output -raw start_build_command | bash
```

Or manually:

```bash
aws imagebuilder start-image-pipeline-execution \
  --image-pipeline-arn $(terraform output -raw image_pipeline_arn) \
  --region $(terraform output -raw aws_region)
```

## Network Requirements

### VPC and Subnet

The subnet must have internet access for downloading packages. You can use:
- **Public subnet** with an Internet Gateway
- **Private subnet** with a NAT Gateway

### Security Group

Your security group should allow:

**Outbound Rules:**
- All traffic to `0.0.0.0/0` (for downloading packages)

**Inbound Rules:**
- None required (Image Builder uses AWS Systems Manager)

Example security group rules:

```bash
# Create security group
aws ec2 create-security-group \
  --group-name imagebuilder-kafka-dev \
  --description "Security group for Kafka Dev Image Builder" \
  --vpc-id vpc-xxxxx

# Allow all outbound traffic (default, but explicit example)
aws ec2 authorize-security-group-egress \
  --group-id sg-xxxxx \
  --protocol all \
  --cidr 0.0.0.0/0
```

## Configuration Options

### Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `aws_region` | No | us-east-1 | AWS region |
| `stack_name` | No | kafka-dev-image-builder | Resource name prefix |
| `vpc_id` | Yes | - | VPC ID |
| `subnet_id` | Yes | - | Subnet ID with internet access |
| `security_group_id` | Yes | - | Security group ID |
| `instance_type` | No | t3.medium | Build instance type |
| `target_regions` | No | [] | Additional regions for AMI |
| `notification_email` | No | "" | Email for build notifications |
| `base_image_id` | No | ami-0c02fb55b7c22fdef | Base Amazon Linux 2 AMI |
| `component_version` | No | 1.0.0 | Component version |
| `ami_user_ids` | No | [] | AWS accounts to share AMI with |

### Finding the Latest Amazon Linux 2 AMI

For your region:

```bash
aws ec2 describe-images \
  --owners amazon \
  --filters "Name=name,Values=amzn2-ami-hvm-*-x86_64-gp2" \
  --query 'sort_by(Images, &CreationDate)[-1].[ImageId, Name, CreationDate]' \
  --output table
```

## Outputs

After applying, Terraform provides these outputs:

```bash
# View all outputs
terraform output

# View specific output
terraform output image_pipeline_arn
terraform output component_arn
terraform output start_build_command
```

### Available Outputs

- `image_pipeline_arn` - Pipeline ARN for starting builds
- `component_arn` - Component ARN for reuse
- `image_recipe_arn` - Recipe ARN
- `infrastructure_config_arn` - Infrastructure configuration ARN
- `distribution_config_arn` - Distribution configuration ARN
- `component_bucket_name` - S3 bucket for logs
- `notification_topic_arn` - SNS topic ARN (if configured)
- `iam_role_arn` - IAM role ARN
- `start_build_command` - Ready-to-run build command

## Monitoring Builds

### Check Build Status

```bash
aws imagebuilder list-image-pipeline-images \
  --image-pipeline-arn $(terraform output -raw image_pipeline_arn) \
  --region $(terraform output -raw aws_region)
```

### View Logs

Logs are stored in the S3 bucket:

```bash
aws s3 ls s3://$(terraform output -raw component_bucket_name)/logs/
```

### Console Access

1. Go to AWS Console → EC2 Image Builder
2. Select "Image pipelines"
3. Click on your pipeline (name from `stack_name` variable)
4. View build history and logs

## Customization

### Modify Kafka Version

Edit the `InstallKafka` step in `main.tf`:

```hcl
commands = [
  "echo 'Installing Apache Kafka...'",
  "cd /tmp",
  "KAFKA_VERSION=\"3.6.1\"",  # Specify exact version
  "SCALA_VERSION=\"2.13\"",
  # ... rest of commands
]
```

Then apply the changes:

```bash
terraform apply
```

### Add Additional Tools

Add new steps to the build phase in the component definition in `main.tf`:

```hcl
{
  name   = "InstallMyTool"
  action = "ExecuteBash"
  inputs = {
    commands = [
      "echo 'Installing my tool...'",
      "yum install -y my-package"
    ]
  }
}
```

### Update Component Version

When making changes to the component:

1. Update `component_version` in `terraform.tfvars`:
   ```hcl
   component_version = "1.0.1"
   ```

2. Apply changes:
   ```bash
   terraform apply
   ```

3. Start a new build with the updated component

## Multi-Region AMI Distribution

To automatically copy AMIs to multiple regions:

```hcl
# In terraform.tfvars
target_regions = ["us-west-2", "eu-west-1", "ap-southeast-1"]
```

After each successful build, the AMI will be automatically copied to these regions.

## AMI Sharing

To share AMIs with other AWS accounts:

```hcl
# In terraform.tfvars
ami_user_ids = ["123456789012", "210987654321"]
```

The AMI will be shared with these accounts after each build.

## Managing State

### Remote State (Recommended for Teams)

Configure S3 backend in `main.tf`:

```hcl
terraform {
  backend "s3" {
    bucket         = "my-terraform-state-bucket"
    key            = "kafka-image-builder/terraform.tfstate"
    region         = "us-east-1"
    encrypt        = true
    dynamodb_table = "terraform-state-locks"
  }
}
```

Then initialize:

```bash
terraform init -migrate-state
```

### Local State (Default)

State is stored in `terraform.tfstate` file locally. Keep this file secure and backed up.

## Cost Estimation

Use Terraform Cloud or Infracost for cost estimates:

```bash
# Install infracost
brew install infracost  # macOS

# Generate cost estimate
infracost breakdown --path .
```

Typical costs:
- Build: ~$0.02 per build (t3.medium for 30 min)
- Storage: ~$0.05/GB/month for AMIs
- S3 logs: Negligible

## Troubleshooting

### Build Failures

1. Check CloudWatch Logs:
   ```bash
   aws logs tail /aws/imagebuilder/KafkaDevEnvironment --follow
   ```

2. Check S3 logs:
   ```bash
   aws s3 ls s3://$(terraform output -raw component_bucket_name)/logs/ --recursive
   ```

3. Verify network connectivity (subnet has internet access)

### Terraform Errors

**Error: Duplicate resource**
```bash
# Import existing resource
terraform import aws_imagebuilder_component.kafka_dev <component-arn>
```

**Error: Bucket already exists**
```bash
# Import existing bucket
terraform import aws_s3_bucket.component_bucket <bucket-name>
```

### State Issues

```bash
# Refresh state
terraform refresh

# Fix state drift
terraform apply -refresh-only
```

## Cleanup

To destroy all resources:

```bash
# Review what will be destroyed
terraform plan -destroy

# Destroy resources
terraform destroy
```

**Note:** This will NOT delete the AMIs that were built. Delete AMIs manually:

```bash
# List AMIs
aws ec2 describe-images \
  --owners self \
  --filters "Name=name,Values=kafka-dev-*" \
  --query 'Images[*].[ImageId, Name, CreationDate]' \
  --output table

# Deregister AMI
aws ec2 deregister-image --image-id ami-xxxxx

# Delete snapshots
aws ec2 describe-snapshots \
  --owner-ids self \
  --filters "Name=description,Values=*kafka-dev*" \
  --query 'Snapshots[*].[SnapshotId]' \
  --output text | xargs -n 1 aws ec2 delete-snapshot --snapshot-id
```

## CI/CD Integration

### GitHub Actions

```yaml
name: Build Kafka AMI

on:
  push:
    branches: [main]
    paths:
      - 'infra/terraform/**'

jobs:
  terraform:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - uses: hashicorp/setup-terraform@v2
        with:
          terraform_version: 1.5.0
      
      - name: Configure AWS Credentials
        uses: aws-actions/configure-aws-credentials@v2
        with:
          aws-access-key-id: ${{ secrets.AWS_ACCESS_KEY_ID }}
          aws-secret-access-key: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
          aws-region: us-east-1
      
      - name: Terraform Init
        run: terraform init
        working-directory: infra/terraform
      
      - name: Terraform Plan
        run: terraform plan
        working-directory: infra/terraform
      
      - name: Terraform Apply
        if: github.ref == 'refs/heads/main'
        run: terraform apply -auto-approve
        working-directory: infra/terraform
```

### GitLab CI

```yaml
image:
  name: hashicorp/terraform:1.5
  entrypoint: [""]

stages:
  - validate
  - plan
  - apply

variables:
  TF_ROOT: infra/terraform

before_script:
  - cd $TF_ROOT
  - terraform init

validate:
  stage: validate
  script:
    - terraform validate

plan:
  stage: plan
  script:
    - terraform plan -out=plan.tfplan
  artifacts:
    paths:
      - $TF_ROOT/plan.tfplan

apply:
  stage: apply
  script:
    - terraform apply plan.tfplan
  when: manual
  only:
    - main
```

## Best Practices

1. **Use remote state** for team collaboration
2. **Pin provider versions** in `main.tf`
3. **Use variables** for all configurable values
4. **Tag all resources** for cost tracking
5. **Version your components** when making changes
6. **Test changes** in a non-production account first
7. **Keep state files secure** (never commit to git)
8. **Document customizations** in comments
9. **Use workspaces** for multiple environments
10. **Regular updates** - rebuild AMIs monthly for security patches

## Support

For issues or questions:
1. Check the main [IMAGE_BUILDER_README.md](../IMAGE_BUILDER_README.md)
2. Review Terraform documentation: https://registry.terraform.io/providers/hashicorp/aws/latest/docs
3. Check AWS Image Builder docs: https://docs.aws.amazon.com/imagebuilder/

## Additional Resources

- [Terraform AWS Provider](https://registry.terraform.io/providers/hashicorp/aws/latest/docs)
- [EC2 Image Builder User Guide](https://docs.aws.amazon.com/imagebuilder/)
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Kafka UI Documentation](https://docs.kafka-ui.provectus.io/)
