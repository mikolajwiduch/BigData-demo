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
  description = "The name of the database created for ETL purposes"
  value       = azurerm_postgresql_flexible_server_database.default_db.name
}

# --- IAM Credentials Outputs ---
output "etl_user_access_key" {
  description = "AWS Access Key ID for the ETL user (Copy to .env as AWS_ACCESS_KEY_ID)"
  value       = aws_iam_access_key.etl_user_key.id
  sensitive   = true
}

output "etl_user_secret_key" {
  description = "AWS Secret Access Key for the ETL user (Copy to .env as AWS_SECRET_ACCESS_KEY)"
  value       = aws_iam_access_key.etl_user_key.secret
  sensitive   = true
}
