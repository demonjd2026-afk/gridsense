# ============================================================================
# Event Hubs Module - Input Variables
# ============================================================================

variable "env" {
  type        = string
  description = "Environment short name (dev, staging, prod)"
}

variable "project" {
  type        = string
  description = "Project name short code"
  default     = "gridsense"
}

variable "rg_name" {
  type        = string
  description = "Resource group to deploy into (from foundation module)"
}

variable "location" {
  type        = string
  description = "Azure region (should match the resource group)"
}

variable "tags" {
  type        = map(string)
  description = "Tags to apply to all Event Hub resources"
  default     = {}
}


variable "databricks_access_connector_principal_id" {
  type        = string
  description = "Principal ID of the Databricks Access Connector. Granted Azure Event Hubs Data Receiver on this namespace so Bronze streaming jobs can consume from all topics."
}


variable "databricks_eh_sp_object_id" {
  type        = string
  description = "Object ID of the Service Principal used by Databricks for EH OAuth. Granted Data Receiver on this namespace (the workaround for Databricks lacking native MI Kafka auth)."
}
