locals {
  suffix   = var.environment == "prod" ? "" : "_${upper(var.environment)}"
  database = "STOCKS_MDS${local.suffix}"
  tags     = { project = "real-time-stocks-mds", environment = var.environment }
}

# ---------------------------------------------------------------- compute
resource "snowflake_warehouse" "stocks" {
  name           = "STOCKS_WH${local.suffix}"
  warehouse_size = var.warehouse_size
  auto_suspend   = var.auto_suspend_seconds
  auto_resume    = true
  initially_suspended = true
  comment        = "Serves dynamic table refreshes and dbt builds."
}

# --------------------------------------------------------------- storage
resource "snowflake_database" "stocks" {
  name                        = local.database
  comment                     = "Real-time stock market pipeline"
  data_retention_time_in_days = var.environment == "prod" ? 7 : 1
}

resource "snowflake_schema" "raw" {
  database = snowflake_database.stocks.name
  name     = "RAW"
  comment  = "Landing zone owned by the Snowflake Kafka Connector. Do not model here."
}

resource "snowflake_schema" "layers" {
  for_each = toset(["BRONZE", "SILVER", "GOLD", "OPS", "DBT_TEST_FAILURES"])

  database = snowflake_database.stocks.name
  name     = each.key
}

# ------------------------------------------------------------------ roles
# Two service identities with different jobs: one can only write to RAW,
# the other can only read RAW and build the modelled layers.
resource "snowflake_role" "ingestion" {
  name    = "STOCKS_INGESTION${local.suffix}"
  comment = "Kafka connector. Write access to RAW only."
}

resource "snowflake_role" "transformer" {
  name    = "STOCKS_TRANSFORMER${local.suffix}"
  comment = "dbt. Reads RAW, owns the modelled schemas."
}

resource "snowflake_role" "reader" {
  name    = "STOCKS_READER${local.suffix}"
  comment = "BI tools. Read-only on GOLD."
}

# ------------------------------------------------------------------ users
resource "snowflake_user" "streaming_svc" {
  name              = "STOCKS_STREAMING_SVC${local.suffix}"
  comment           = "Service user for the Snowflake Kafka Connector."
  default_role      = snowflake_role.ingestion.name
  default_warehouse = snowflake_warehouse.stocks.name
  # Key-pair auth is mandatory for Snowpipe Streaming; there is no password.
  rsa_public_key    = var.streaming_public_key
  must_change_password = false
}

# ----------------------------------------------------------------- grants
resource "snowflake_grant_privileges_to_account_role" "warehouse_usage" {
  for_each = toset([
    snowflake_role.ingestion.name,
    snowflake_role.transformer.name,
    snowflake_role.reader.name,
  ])

  account_role_name = each.key
  privileges        = ["USAGE", "OPERATE"]

  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.stocks.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "database_usage" {
  for_each = toset([
    snowflake_role.ingestion.name,
    snowflake_role.transformer.name,
    snowflake_role.reader.name,
  ])

  account_role_name = each.key
  privileges        = ["USAGE"]

  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.stocks.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "ingestion_raw" {
  account_role_name = snowflake_role.ingestion.name
  privileges        = ["USAGE", "CREATE TABLE", "CREATE STAGE", "CREATE PIPE"]

  on_schema {
    schema_name = "\"${snowflake_database.stocks.name}\".\"${snowflake_schema.raw.name}\""
  }
}

resource "snowflake_grant_privileges_to_account_role" "transformer_reads_raw" {
  account_role_name = snowflake_role.transformer.name
  privileges        = ["USAGE"]

  on_schema {
    schema_name = "\"${snowflake_database.stocks.name}\".\"${snowflake_schema.raw.name}\""
  }
}

resource "snowflake_grant_privileges_to_account_role" "transformer_owns_layers" {
  for_each = snowflake_schema.layers

  account_role_name = snowflake_role.transformer.name
  privileges = [
    "USAGE", "CREATE TABLE", "CREATE VIEW",
    "CREATE DYNAMIC TABLE", "CREATE MATERIALIZED VIEW",
  ]

  on_schema {
    schema_name = "\"${snowflake_database.stocks.name}\".\"${each.value.name}\""
  }
}

resource "snowflake_grant_privileges_to_account_role" "reader_gold" {
  account_role_name = snowflake_role.reader.name
  privileges        = ["SELECT"]

  on_schema_object {
    future {
      object_type_plural = "TABLES"
      in_schema          = "\"${snowflake_database.stocks.name}\".\"GOLD\""
    }
  }
}

resource "snowflake_grant_account_role" "streaming_svc_ingestion" {
  role_name = snowflake_role.ingestion.name
  user_name = snowflake_user.streaming_svc.name
}
