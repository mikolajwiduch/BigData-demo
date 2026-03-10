resource "random_string" "suffix" {
  length  = 6
  special = false
  upper   = false
}

# ---# AWS S3 Bucket for Raw Data lake
resource "aws_s3_bucket" "raw_data" {
  bucket_prefix = "${var.project_prefix}-raw-data-"
  force_destroy = true
}

# --- AWS IAM setup for ETL Script ---

# User dedicated for ETL tasks
resource "aws_iam_user" "etl_user" {
  name = "${var.project_prefix}-etl-user"
  path = "/system/"
}

# Generate access keys for the ETL User
resource "aws_iam_access_key" "etl_user_key" {
  user = aws_iam_user.etl_user.name
}

# IAM Policy document restricting access to only this specific S3 bucket
data "aws_iam_policy_document" "etl_s3_policy" {
  statement {
    effect = "Allow"
    actions = [
      "s3:ListBucket"
    ]
    resources = [
      aws_s3_bucket.raw_data.arn
    ]
  }

  statement {
    effect = "Allow"
    actions = [
      "s3:PutObject",
      "s3:GetObject"
    ]
    resources = [
      "${aws_s3_bucket.raw_data.arn}/*"
    ]
  }
}

# Attach policy to the ETL User
resource "aws_iam_user_policy" "etl_user_s3_access" {
  name   = "etl_s3_access"
  user   = aws_iam_user.etl_user.name
  policy = data.aws_iam_policy_document.etl_s3_policy.json
}
# ------------------------------------

# --- Azure: Resource Group ---
resource "azurerm_resource_group" "rg" {
  name     = "${var.project_prefix}-rg"
  location = var.azure_location
}

# --- Azure: PostgreSQL Flexible Server ---
resource "azurerm_postgresql_flexible_server" "db" {
  name                          = "${var.project_prefix}-psql-${random_string.suffix.result}"
  resource_group_name           = azurerm_resource_group.rg.name
  location                      = azurerm_resource_group.rg.location
  version                       = "16"
  administrator_login           = var.db_admin_user
  administrator_password        = var.db_admin_password
  storage_mb                    = 32768
  sku_name                      = "B_Standard_B1ms"
  public_network_access_enabled = true

  # Ignore zone changes as Azure sometimes manages it dynamically leading to Terraform drifts.
  lifecycle {
    ignore_changes = [
      zone,
      high_availability.0.standby_availability_zone
    ]
  }
}

# Allow external access (since this is a demo, allowing all IPs. In real life we'd restrict to specific IPs)
resource "azurerm_postgresql_flexible_server_firewall_rule" "allow_all" {
  name             = "AllowAll"
  server_id        = azurerm_postgresql_flexible_server.db.id
  start_ip_address = "0.0.0.0"
  end_ip_address   = "255.255.255.255"
}

resource "azurerm_postgresql_flexible_server_database" "default_db" {
  name      = "etl_demo_db"
  server_id = azurerm_postgresql_flexible_server.db.id
  collation = "en_US.utf8"
  charset   = "utf8"
}
