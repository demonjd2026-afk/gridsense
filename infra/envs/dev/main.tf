# ============================================================================
# Dev Environment Composition
# Wires together foundation, eventhubs, databricks, and container-apps modules.
# ============================================================================

terraform {
  required_version = ">= 1.7"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.110"
    }
    azuread = {
      source  = "hashicorp/azuread"
      version = "~> 2.53"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }

  backend "azurerm" {
    resource_group_name  = "rg-gridsense-tfstate"
    storage_account_name = "sttfstategs21126"
    container_name       = "tfstate"
    key                  = "dev.tfstate"
  }
}

provider "azurerm" {
  features {
    key_vault {
      purge_soft_delete_on_destroy    = true
      recover_soft_deleted_key_vaults = true
    }
    resource_group {
      prevent_deletion_if_contains_resources = false
    }
  }
}

# AzureAD provider for Service Principal management.
# Required permissions on the executing identity: Application.ReadWrite.OwnedBy
# (or Application.ReadWrite.All for org-wide SP management).
provider "azuread" {}

locals {
  env      = "dev"
  location = "centralindia"
  project  = "gridsense"
}

# ----------------------------------------------------------------------------
# Foundation: resource group, storage, key vault, log analytics, access connector
# ----------------------------------------------------------------------------
module "foundation" {
  source = "../../modules/foundation"

  env      = local.env
  location = local.location
  project  = local.project

  # Pin Key Vault admin to a stable user object id so that local and CI
  # applies agree on the desired state. Replace with another user/SP
  # object_id if Jay leaves the project.
  kv_admin_principal_id = "3a075b46-9060-4569-b40b-458068cf399a" # jay_do (jayanthdolai@gmail.com)
}

# ----------------------------------------------------------------------------
# Event Hubs: namespace + 3 topics + consumer groups
# ----------------------------------------------------------------------------
module "eventhubs" {
  source = "../../modules/eventhubs"

  env      = local.env
  project  = local.project
  rg_name  = module.foundation.resource_group_name
  location = module.foundation.location
  tags     = module.foundation.tags

  databricks_access_connector_principal_id = module.foundation.access_connector_principal_id
  databricks_eh_sp_object_id               = module.foundation.databricks_eh_sp_object_id
}

# ----------------------------------------------------------------------------
# Databricks: workspace
# ----------------------------------------------------------------------------
module "databricks" {
  source = "../../modules/databricks"

  env      = local.env
  project  = local.project
  rg_name  = module.foundation.resource_group_name
  location = module.foundation.location
  tags     = module.foundation.tags
}

# ----------------------------------------------------------------------------
# Container Apps: ACR + environment + UAMI + role assignments for producers
# ----------------------------------------------------------------------------
module "container_apps" {
  source = "../../modules/container-apps"

  env                        = local.env
  project                    = local.project
  rg_name                    = module.foundation.resource_group_name
  location                   = module.foundation.location
  tags                       = module.foundation.tags
  log_analytics_workspace_id = module.foundation.log_analytics_workspace_id
  eventhubs_namespace_id     = module.eventhubs.namespace_id
  eventhubs_namespace_name   = module.eventhubs.namespace_name
  key_vault_id               = module.foundation.key_vault_id
  key_vault_uri              = module.foundation.key_vault_uri
  random_suffix              = module.foundation.random_suffix

  producers = {
    "carbon-intensity" = {
      image_repo      = "carbon-intensity"
      image_tag       = "v10"
      eventhub_topic  = "carbon-intensity"
      poll_interval_s = 300
    }
    "open-meteo" = {
      image_repo      = "open-meteo"
      image_tag       = "v2"
      eventhub_topic  = "open-meteo"
      poll_interval_s = 900
    }
    "entsoe" = {
      image_repo      = "entsoe"
      image_tag       = "v1"
      eventhub_topic  = "entsoe"
      poll_interval_s = 3600
      secrets = {
        "ENTSOE_API_TOKEN" = "entsoe-api-token"
      }
    }
  }
}

# ----------------------------------------------------------------------------
# GitHub Actions OIDC: SP + federated credentials for CI/CD
# ----------------------------------------------------------------------------
module "github_oidc" {
  source = "../../modules/github_oidc"

  env         = local.env
  project     = local.project
  github_org  = "demonjd2026-afk"
  github_repo = "gridsense"

  # State storage account from the backend block (hardcoded here because
  # the backend block can't reference resources or outputs).
  tfstate_storage_account_id = "/subscriptions/1262ba1e-e555-43f6-a5a6-d61c2c3abf3b/resourceGroups/rg-gridsense-tfstate/providers/Microsoft.Storage/storageAccounts/sttfstategs21126"

  # Key Vault for data-plane access (read secrets in CI)
  key_vault_id = module.foundation.key_vault_id
}

# ----------------------------------------------------------------------------
# Outputs
# ----------------------------------------------------------------------------
output "resource_group_name" {
  value = module.foundation.resource_group_name
}

output "storage_account_name" {
  value = module.foundation.storage_account_name
}

output "storage_dfs_endpoint" {
  value = module.foundation.storage_dfs_endpoint
}

output "key_vault_uri" {
  value = module.foundation.key_vault_uri
}

output "access_connector_id" {
  value = module.foundation.access_connector_id
}

output "eventhubs_namespace" {
  value = module.eventhubs.namespace_name
}

output "kafka_bootstrap_server" {
  value = module.eventhubs.kafka_bootstrap_server
}

output "databricks_workspace_url" {
  value = module.databricks.workspace_url
}

output "acr_login_server" {
  value = module.container_apps.acr_login_server
}

output "container_app_environment_id" {
  value = module.container_apps.container_app_environment_id
}

output "producers_uami_id" {
  value = module.container_apps.producers_uami_id
}

output "producers_uami_client_id" {
  value = module.container_apps.producers_uami_client_id
}

# ----------------------------------------------------------------------------
# GitHub Actions OIDC outputs
# ----------------------------------------------------------------------------
output "github_actions_client_id" {
  description = "Set as AZURE_CLIENT_ID secret in GitHub repo settings"
  value       = module.github_oidc.client_id
}

output "github_actions_sp_name" {
  description = "Display name of the GitHub Actions SP (for audit/lookup)"
  value       = module.github_oidc.app_registration_display_name
}
