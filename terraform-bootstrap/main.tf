terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = "eu-central-1"
}

resource "random_string" "suffix" {
  length  = 6
  special = false
  upper   = false
}

resource "aws_s3_bucket" "tfstate" {
  bucket = "bigdata-demo-tfstate-${random_string.suffix.result}"
}

output "state_bucket_name" {
  value = aws_s3_bucket.tfstate.bucket
}
