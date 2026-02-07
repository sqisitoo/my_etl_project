terraform {
    required_providers {
        aws = {
            source = "hashicorp/aws"
            version = "~> 5.0"
        }
    }

    backend "s3" {}
}

provider "aws" {
    region = var.aws_region
    profile = var.aws_profile

    default_tags {
        tags = {
      Project = "MyPetProject"
      Environment = "Dev"
      ManagedBy = "Terraform"
        }
    }
}