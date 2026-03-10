resource "random_string" "suffix" {
  length  = 6
  special = false
  upper   = false
}

# --- AWS: S3 Bucket ---
resource "aws_s3_bucket" "raw_data" {
  bucket        = "${var.project_prefix}-raw-data-${random_string.suffix.result}"
  force_destroy = true
}

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
