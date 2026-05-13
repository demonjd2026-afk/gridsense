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

variable "eventhubs_namespace_name" {
  type        = string
  description = "Event Hubs namespace name (used in producer env vars)"
}

# ----------------------------------------------------------------------------
# Producer Container Apps
# Each entry creates one azurerm_container_app. The image must already exist
# in the ACR (built via `az acr build`); Terraform only manages the app
# definition, not the image.
# ----------------------------------------------------------------------------
variable "producers" {
  type = map(object({
    image_repo      = string # e.g. "carbon-intensity"
    image_tag       = string # e.g. "v10"
    eventhub_topic  = string # e.g. "carbon-intensity"
    poll_interval_s = number # e.g. 300
    cpu             = optional(number, 0.25)
    memory          = optional(string, "0.5Gi")
    min_replicas    = optional(number, 1)
    max_replicas    = optional(number, 1)
    # Optional Key Vault-backed secrets. Map of env-var-name -> KV secret name.
    # Each entry creates a secret block on the Container App (resolved at
    # runtime via the producers UAMI) and exposes it as the named env var.
    secrets = optional(map(string), {})
  }))
  description = "Map of producer name -> config. Key becomes the Container App name suffix."
  default     = {}
}

variable "key_vault_id" {
  type        = string
  description = "Key Vault resource ID. UAMI gets 'Key Vault Secrets User' on this vault so Container Apps can mount secrets at runtime."
}

variable "key_vault_uri" {
  type        = string
  description = "Key Vault DNS URI (https://...vault.azure.net/) for building secret references."
}
