# ============================================================================
# Databricks Module - Input Variables
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
  description = "Tags to apply to all Databricks resources"
  default     = {}
}
