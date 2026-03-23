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

# Calling the module
module "my_lakehouse" {
  source = "./modules/lakehouse"
  project_name = "ev-fleet"
}