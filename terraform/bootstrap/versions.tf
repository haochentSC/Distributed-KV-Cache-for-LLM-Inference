# Bootstrap uses LOCAL state on purpose: it CREATES the S3 bucket + DynamoDB table that the
# cluster module then uses as its remote backend. A remote backend can't store the resources that
# define the remote backend (chicken-and-egg), so this one module keeps its small state file local.
terraform {
  required_version = ">= 1.6"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.region
  default_tags {
    tags = {
      project = "kvcache"
      phase   = "4"
      owner   = "hc"
      module  = "bootstrap"
    }
  }
}
