terraform {
  backend "s3" {
    bucket = "bigdata-demo-tfstate-bo8wyf"
    key    = "terraform/state/terraform.tfstate"
    region = "eu-central-1"
  }
}
