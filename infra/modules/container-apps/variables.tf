# ============================================================================
# Container Apps Module - Input Variables
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
  description = "Tags to apply to all resources"
  default     = {}
}

variable "log_analytics_workspace_id" {
  type        = string
  description = "Log Analytics workspace resource ID (for Container Apps env logs)"
}

variable "eventhubs_namespace_id" {
  type        = string
  description = "Event Hubs namespace resource ID (for role assignment scope)"
}

variable "random_suffix" {
  type        = string
  description = "Random suffix for globally-unique resource names (from foundation)"
}
