terraform {
  backend "s3" {
    bucket         = "ev-fleet-terraform-state-1774257402"
    key            = "state/terraform.tfstate"
    region         = "eu-west-1"
    encrypt        = true
    dynamodb_table = "terraform-state-lock"
  }
}

provider "aws" {
  region  = "eu-west-1"
  profile = "personal"
}

# 1. The Database Module
module "database" {
  source           = "./modules/database"
  project_name     = var.project_name
  lambda_role_name = module.lakehouse.lambda_role_name  # The final wire!
}

module "lakehouse" {
  source              = "./modules/lakehouse"
  project_name        = var.project_name
  dynamodb_table_name = module.database.table_name # Passing the output
}

module "compute" {
  source       = "./modules/compute"
  project_name = var.project_name
  
  # Wiring from Database
  dynamodb_table_name = module.database.table_name

  # Wiring from Lakehouse
  lambda_role_arn           = module.lakehouse.lambda_role_arn
  lambda_role_id            = module.lakehouse.lambda_role_id
  silver_bucket_arn         = module.lakehouse.silver_bucket_arn
  gold_bucket_arn           = module.lakehouse.gold_bucket_arn
  gold_bucket_id            = module.lakehouse.gold_bucket_id
  athena_results_bucket_arn = module.lakehouse.athena_results_bucket_arn
  athena_workgroup_name     = module.lakehouse.athena_workgroup_name
  glue_database_name        = module.lakehouse.glue_database_name
}