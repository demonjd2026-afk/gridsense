# ============================================================================
# Foundation Module - Input Variables
# ============================================================================

variable "env" {
  type        = string
  description = "Environment short name (dev, staging, prod)"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.env)
    error_message = "env must be one of: dev, staging, prod."
  }
}

variable "location" {
  type        = string
  description = "Azure region to deploy into"
  default     = "centralindia"
}

variable "project" {
  type        = string
  description = "Project name short code (becomes part of resource names)"
  default     = "gridsense"

  validation {
    condition     = length(var.project) <= 12 && can(regex("^[a-z0-9]+$", var.project))
    error_message = "project must be lowercase alphanumeric and <= 12 chars (storage account name has a 24-char limit)."
  }
}

variable "extra_tags" {
  type        = map(string)
  description = "Additional tags to merge into the default tag set"
  default     = {}
}
