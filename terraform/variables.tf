variable "snowflake_account" {
  type        = string
  description = "Snowflake account locator, e.g. xy12345.eu-west-1"
}

variable "snowflake_admin_user" {
  type        = string
  description = "User running Terraform. Needs ACCOUNTADMIN."
}

variable "environment" {
  type        = string
  description = "Environment suffix: dev, ci or prod."
  default     = "dev"

  validation {
    condition     = contains(["dev", "ci", "prod"], var.environment)
    error_message = "environment must be one of dev, ci, prod."
  }
}

variable "streaming_public_key" {
  type        = string
  description = "PKCS#8 public key body for the Kafka connector service user."
  sensitive   = true
}

variable "warehouse_size" {
  type        = string
  description = "Warehouse size. XSMALL is enough for this workload."
  default     = "XSMALL"
}

variable "auto_suspend_seconds" {
  type        = number
  description = "Warehouse idle timeout. Keep low: dynamic tables wake it every minute anyway."
  default     = 60
}
