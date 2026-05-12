# ============================================================================
# Foundation Module
# Provisions: resource group, ADLS Gen2 storage with 4 containers, Key Vault,
# Log Analytics workspace, and a Databricks Access Connector (managed identity).
# Everything else in the project depends on these resources.
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
}

# ----------------------------------------------------------------------------
# Locals: derived values used across resources
# ----------------------------------------------------------------------------
locals {
  rg_name      = "rg-${var.project}-${var.env}"
  storage_name = "st${var.project}${var.env}${random_string.suffix.result}"
  kv_name      = "kv-${var.project}-${var.env}-${random_string.suffix.result}"
  la_name      = "la-${var.project}-${var.env}"
  dac_name     = "dac-${var.project}-${var.env}"

  tags = merge({
    project     = var.project
    environment = var.env
    managed_by  = "terraform"
    cost_center = "portfolio"
  }, var.extra_tags)
}

# 6-char random suffix to avoid name collisions on globally-unique resources
resource "random_string" "suffix" {
  length  = 6
  upper   = false
  special = false
  numeric = true
}

# ----------------------------------------------------------------------------
# Resource Group: parent container for almost everything else
# ----------------------------------------------------------------------------
resource "azurerm_resource_group" "this" {
  name     = local.rg_name
  location = var.location
  tags     = local.tags
}

# ----------------------------------------------------------------------------
# ADLS Gen2 Storage Account
# Hierarchical namespace = true is what makes it Gen2 (not classic blob).
# Hosts the bronze/silver/gold/metastore zones as filesystems (containers).
# ----------------------------------------------------------------------------
resource "azurerm_storage_account" "lake" {
  name                     = local.storage_name
  resource_group_name      = azurerm_resource_group.this.name
  location                 = var.location
  account_tier             = "Standard"
  account_replication_type = "LRS" # cheapest; portfolio scope, not multi-region

  is_hns_enabled                = true # critical: HNS = ADLS Gen2
  min_tls_version               = "TLS1_2"
  public_network_access_enabled = true # tighten in prod with private endpoints

  # Allow shared-key access during dev so Storage Explorer / az CLI work easily.
  # In prod you'd set this to false and use Azure AD / managed identity only.
  shared_access_key_enabled = true

  blob_properties {
    delete_retention_policy {
      days = 7
    }
  }

  tags = local.tags
}

# Bronze, silver, gold, metastore "filesystems" (Gen2 containers)
resource "azurerm_storage_data_lake_gen2_filesystem" "zones" {
  for_each           = toset(["bronze", "silver", "gold", "metastore"])
  name               = each.key
  storage_account_id = azurerm_storage_account.lake.id
}

# ----------------------------------------------------------------------------
# Key Vault: for secrets (OpenAI key, ENTSO-E token, etc.)
# RBAC-based (modern) instead of access policies (legacy).
# ----------------------------------------------------------------------------
data "azurerm_client_config" "current" {}

resource "azurerm_key_vault" "this" {
  name                = local.kv_name
  location            = var.location
  resource_group_name = azurerm_resource_group.this.name
  tenant_id           = data.azurerm_client_config.current.tenant_id
  sku_name            = "standard"

  enable_rbac_authorization     = true
  purge_protection_enabled      = false # set true in prod; portfolio = recoverable for cost
  soft_delete_retention_days    = 7
  public_network_access_enabled = true

  tags = local.tags
}

# Grant the deploying user (you) "Key Vault Administrator" so you can manage secrets via portal/CLI
resource "azurerm_role_assignment" "kv_admin_user" {
  scope                = azurerm_key_vault.this.id
  role_definition_name = "Key Vault Administrator"
  principal_id         = data.azurerm_client_config.current.object_id
}

# ----------------------------------------------------------------------------
# Log Analytics Workspace: destination for all logs (Container Apps, Databricks, etc.)
# ----------------------------------------------------------------------------
resource "azurerm_log_analytics_workspace" "this" {
  name                = local.la_name
  location            = var.location
  resource_group_name = azurerm_resource_group.this.name
  sku                 = "PerGB2018"
  retention_in_days   = 30
  tags                = local.tags
}

# ----------------------------------------------------------------------------
# Databricks Access Connector (managed identity)
# This is HOW Databricks authenticates to ADLS Gen2 without storing keys.
# We grant it Storage Blob Data Contributor on the lake account.
# ----------------------------------------------------------------------------
resource "azurerm_databricks_access_connector" "this" {
  name                = local.dac_name
  resource_group_name = azurerm_resource_group.this.name
  location            = var.location

  identity {
    type = "SystemAssigned"
  }

  tags = local.tags
}

# Grant the access connector full access to the storage account
resource "azurerm_role_assignment" "access_connector_storage" {
  scope                = azurerm_storage_account.lake.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.this.identity[0].principal_id
}
