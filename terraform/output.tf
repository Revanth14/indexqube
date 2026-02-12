
output "vpc_id" {
  description = "VPC ID"
  value       = module.network.vpc_id
}

output "rds_endpoint" {
  description = "RDS endpoint"
  value       = module.rds.rds_endpoint
}

output "rds_database_name" {
  description = "RDS database name"
  value       = module.rds.rds_database_name
}
output "s3_raw_bucket" {
  description = "S3 raw bucket"
  value       = module.s3.s3_raw_bucket
}

output "s3_curated_bucket" {
  description = "S3 curated bucket"
  value       = module.s3.s3_curated_bucket
}

output "s3_gold_bucket" {
  description = "S3 gold bucket"
  value       = module.s3.s3_gold_bucket
}