output "database" {
  value       = snowflake_database.stocks.name
  description = "Set this as SNOWFLAKE_DATABASE."
}

output "warehouse" {
  value       = snowflake_warehouse.stocks.name
  description = "Set this as SNOWFLAKE_WAREHOUSE."
}

output "ingestion_role" {
  value       = snowflake_role.ingestion.name
  description = "Set this as snowflake.role.name in the connector config."
}

output "transformer_role" {
  value       = snowflake_role.transformer.name
  description = "Set this as SNOWFLAKE_ROLE for dbt."
}

output "streaming_user" {
  value       = snowflake_user.streaming_svc.name
  description = "Set this as snowflake.user.name in the connector config."
}
