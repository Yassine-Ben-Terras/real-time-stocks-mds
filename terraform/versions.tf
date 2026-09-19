terraform {
  required_version = ">= 1.6.0"

  required_providers {
    snowflake = {
      source  = "Snowflake-Labs/snowflake"
      version = "~> 0.94"
    }
  }

  # Swap to an S3/GCS backend before using this for anything shared.
  backend "local" {
    path = "terraform.tfstate"
  }
}

provider "snowflake" {
  account = var.snowflake_account
  user    = var.snowflake_admin_user
  role    = "ACCOUNTADMIN"
}
