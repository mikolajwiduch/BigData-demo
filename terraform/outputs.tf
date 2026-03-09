output "s3_bucket_name" {
  description = "Name of the AWS S3 Bucket for raw data"
  value       = aws_s3_bucket.raw_data.bucket
}

output "postgres_server_name" {
  description = "Azure PostgreSQL Server Name"
  value       = azurerm_postgresql_flexible_server.db.name
}

output "postgres_fqdn" {
  description = "Azure PostgreSQL Server FQDN"
  value       = azurerm_postgresql_flexible_server.db.fqdn
}

output "postgres_database_name" {
  description = "Azure PostgreSQL Database Name"
  value       = azurerm_postgresql_flexible_server_database.default_db.name
}
