terraform {
  required_version = ">= 1.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project = var.project_name
    }
  }
}

module "network" {
  source = "./modules/network"

  aws_region           = var.aws_region
  project_name         = var.project_name
  vpc_cidr             = var.vpc_cidr
  public_subnet_cidrs  = var.public_subnet_cidrs
  private_subnet_cidrs = var.private_subnet_cidrs
}

module "rds" {
  source = "./modules/rds"

  project_name               = var.project_name
  vpc_id                     = module.network.vpc_id
  vpc_cidr                   = var.vpc_cidr
  private_subnet_ids         = module.network.private_subnet_ids
  db_name                    = var.db_name
  db_username                = var.db_username
  db_password                = var.db_password
  db_instance_class          = var.db_instance_class
  db_allocated_storage       = var.db_allocated_storage
  db_max_allocated_storage   = var.db_max_allocated_storage
  db_backup_retention_period = var.db_backup_retention_period
  db_multi_az                = var.db_multi_az
  db_publicly_accessible     = var.db_publicly_accessible
  db_deletion_protection     = var.db_deletion_protection
  public_subnet_ids          = module.network.public_subnet_ids
}

module "s3" {
  source = "./modules/s3"

  project_name = var.project_name
}
