variable "project_name" {
  description = "Project name"
  type        = string
}

variable "vpc_id" {
  description = "VPC ID"
  type        = string
}

variable "vpc_cidr" {
  description = "VPC CIDR block"
  type        = string
}

variable "private_subnet_ids" {
  description = "Private subnet IDs"
  type        = list(string)
}

variable "db_name" {
  description = "RDS database name"
  type        = string
}

variable "db_username" {
  description = "RDS database username"
  type        = string
}

variable "db_password" {
  description = "RDS database password"
  type        = string
  sensitive   = true
}

variable "db_instance_class" {
  description = "RDS instance class"
  type        = string
}

variable "db_allocated_storage" {
  description = "Initial RDS storage in GB"
  type        = number
}

variable "db_max_allocated_storage" {
  description = "Maximum RDS storage in GB for autoscaling"
  type        = number
}

variable "db_backup_retention_period" {
  description = "RDS backup retention in days"
  type        = number
}

variable "db_multi_az" {
  description = "Enable multi-AZ for RDS"
  type        = bool
}

variable "db_publicly_accessible" {
  description = "Make RDS publicly accessible"
  type        = bool
  default = true
}

variable "db_deletion_protection" {
  description = "Enable RDS deletion protection"
  type        = bool
}

variable "public_subnet_ids" {
  description = "Public subnet IDs for RDS"
  type        = list(string)
}