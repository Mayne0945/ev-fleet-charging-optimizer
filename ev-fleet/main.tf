provider "aws" {
  region  = "eu-west-1"
  profile = "personal"
}

# Calling the module
module "my_lakehouse" {
  source       = "../modules"
  project_name = "ev-fleet"
}

# showing the bucket names in the terminal after it finishes
output "my_buckets" {
  value = module.my_lakehouse.bucket_ids
}
output "api_url" {
  # I can see in your logs your module is named "my_lakehouse"
  value = module.my_lakehouse.api_endpoint
}