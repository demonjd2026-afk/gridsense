# ============================================================================
# Dev Environment Composition
# Wires together the foundation, eventhubs, and databricks modules.
# This is what `terraform apply` is run against for the dev environment.
# ============================================================================

terraform {
  required_version = ">= 1.7"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.110"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }

  # Remote state in Azure Storage (bootstrapped manually before first apply)
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

# ----------------------------------------------------------------------------
# Locals
# ----------------------------------------------------------------------------
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
# Outputs surfaced from this env (useful for `terraform output` debugging
# and for CI/CD to pick up downstream-needed values)
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
