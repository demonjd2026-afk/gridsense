# ============================================================================
# Databricks Module
# Provisions: Databricks workspace (Premium tier required for Unity Catalog,
# DLT serverless, and certain security features).
#
# NOTE: The Unity Catalog metastore is an account-level resource that must be
# created once via the accounts portal (https://accounts.azuredatabricks.net).
# That step happens in Phase 3, after this workspace is up.
# ============================================================================

terraform {
  required_version = ">= 1.7"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.110"
    }
  }
}

locals {
  workspace_name = "dbw-${var.project}-${var.env}"
  # Managed RG holds Databricks's internal resources (VMs, NICs, NSG, etc.)
  managed_rg_name = "databricks-rg-${local.workspace_name}-${var.env}"
}

# ----------------------------------------------------------------------------
# Databricks Workspace
# Premium tier: needed for Unity Catalog, DLT, role-based access, etc.
# ----------------------------------------------------------------------------
resource "azurerm_databricks_workspace" "this" {
  name                = local.workspace_name
  resource_group_name = var.rg_name
  location            = var.location
  sku                 = "premium"

  # Databricks needs its own managed RG for the underlying VMs/network.
  managed_resource_group_name = local.managed_rg_name

  # Public network: ok for portfolio. Prod would use a VNet-injected workspace
  # (Secure Cluster Connectivity + Private Endpoints), which costs more.
  public_network_access_enabled = true

  tags = var.tags
}
