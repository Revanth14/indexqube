output "s3_raw_bucket" {
  description = "S3 raw bucket"
  value       = aws_s3_bucket.raw.bucket
}

output "s3_curated_bucket" {
  description = "S3 curated bucket"
  value       = aws_s3_bucket.curated.bucket
}

output "s3_gold_bucket" {
  description = "S3 gold bucket"
  value       = aws_s3_bucket.gold.bucket
}
