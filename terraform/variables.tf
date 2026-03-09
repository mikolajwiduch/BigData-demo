variable "aws_region" {
  type    = string
  default = "eu-central-1"
}

variable "azure_location" {
  type    = string
  default = "West Europe"
}

variable "db_admin_user" {
  type    = string
  default = "psqladmin"
}

variable "db_admin_password" {
  type      = string
  sensitive = true
}

variable "project_prefix" {
  type    = string
  default = "bigdata-demo"
}
