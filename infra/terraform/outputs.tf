output "image_pipeline_arn" {
  description = "ARN of the Image Builder Pipeline"
  value       = aws_imagebuilder_image_pipeline.kafka_dev.arn
}

output "component_arn" {
  description = "ARN of the Image Builder Component"
  value       = aws_imagebuilder_component.kafka_dev.arn
}

output "image_recipe_arn" {
  description = "ARN of the Image Recipe"
  value       = aws_imagebuilder_image_recipe.kafka_dev.arn
}

output "infrastructure_config_arn" {
  description = "ARN of the Infrastructure Configuration"
  value       = aws_imagebuilder_infrastructure_configuration.kafka_dev.arn
}

output "distribution_config_arn" {
  description = "ARN of the Distribution Configuration"
  value       = aws_imagebuilder_distribution_configuration.kafka_dev.arn
}

output "component_bucket_name" {
  description = "S3 Bucket for Image Builder components and logs"
  value       = aws_s3_bucket.component_bucket.id
}

output "component_bucket_arn" {
  description = "S3 Bucket ARN for Image Builder components and logs"
  value       = aws_s3_bucket.component_bucket.arn
}

output "notification_topic_arn" {
  description = "SNS Topic for build notifications"
  value       = var.notification_email != "" ? aws_sns_topic.notifications[0].arn : null
}

output "iam_role_arn" {
  description = "IAM Role ARN for Image Builder"
  value       = aws_iam_role.image_builder.arn
}

output "iam_instance_profile_arn" {
  description = "IAM Instance Profile ARN for Image Builder"
  value       = aws_iam_instance_profile.image_builder.arn
}

output "start_build_command" {
  description = "AWS CLI command to start an image build"
  value       = "aws imagebuilder start-image-pipeline-execution --image-pipeline-arn ${aws_imagebuilder_image_pipeline.kafka_dev.arn} --region ${var.aws_region}"
}

output "component_version" {
  description = "Version of the Image Builder component"
  value       = var.component_version
}

output "base_image_id" {
  description = "Base AMI ID used for the image"
  value       = var.base_image_id
}
