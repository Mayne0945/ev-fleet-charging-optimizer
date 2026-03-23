provider "aws" {
  region  = "eu-west-1"
  profile = "personal"
}

# Calling the module
module "my_lakehouse" {
  source = "./modules/lakehouse"
  project_name = "ev-fleet"
}