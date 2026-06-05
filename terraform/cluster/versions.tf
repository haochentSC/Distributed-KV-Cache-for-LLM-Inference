terraform {
  required_version = ">= 1.6"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # Remote state in the bucket+table created by ../bootstrap. Backends can't read variables, so the
  # bucket/region/table are supplied at init time:
  #   terraform init -backend-config=backend.hcl
  # (copy backend.hcl.example -> backend.hcl and fill in the bootstrap outputs).
  backend "s3" {
    key     = "cluster/terraform.tfstate"
    encrypt = true
  }
}

provider "aws" {
  region = var.region
  default_tags {
    tags = {
      project = "kvcache"
      phase   = "4"
      owner   = "hc"
      module  = "cluster"
    }
  }
}
