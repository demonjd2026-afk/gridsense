# ============================================================================
# GitHub Actions OIDC Federation Module
# ----------------------------------------------------------------------------
# Creates the Azure-side trust relationship needed for GitHub Actions to
# authenticate to Azure without storing any client secrets.
#
# What it provisions:
#   - Azure AD application + service principal
#   - Three federated credentials on the app:
#       1. Pull requests       (read-only plans on PRs)
#       2. Push to main        (apply on merges to main)
#       3. Workflow environments (reserved for production gating later)
#   - RBAC role assignments:
#       - Contributor on the subscription            (for resource CRUD)
#       - Storage Blob Data Contributor on tfstate   (for backend state)
#
# Trust chain at runtime:
#   GitHub Actions run -> presents short-lived OIDC JWT -> Azure AD validates
#   the JWT's "sub" claim against this federated credential -> issues a
#   short-lived access token. No secrets in transit, no secrets at rest.
# ============================================================================

terraform {
  required_providers {
    azurerm = { source = "hashicorp/azurerm", version = "~> 3.110" }
    azuread = { source = "hashicorp/azuread", version = "~> 2.53" }
  }
}

data "azurerm_client_config" "current" {}
data "azurerm_subscription" "current" {}

# ----------------------------------------------------------------------------
# App registration + service principal
# ----------------------------------------------------------------------------
resource "azuread_application" "gh_oidc" {
  display_name = "${var.project}-github-actions-${var.env}"

  feature_tags {
    enterprise = false
    gallery    = false
    hide       = false
  }
}

resource "azuread_service_principal" "gh_oidc" {
  client_id = azuread_application.gh_oidc.client_id

  description = "Service principal used by GitHub Actions in ${var.github_org}/${var.github_repo} to deploy ${var.project} infrastructure via OIDC federation. No client secret."
}

# ----------------------------------------------------------------------------
# Federated identity credentials
# ----------------------------------------------------------------------------
# Each credential pins on a specific OIDC "subject" claim that GitHub sends
# in the JWT. We register three because GitHub uses different subjects for
# different workflow trigger types.

resource "azuread_application_federated_identity_credential" "pull_request" {
  application_id = azuread_application.gh_oidc.id
  display_name   = "github-pr"
  description    = "PR workflows in ${var.github_org}/${var.github_repo} (terraform plan only)"
  audiences      = ["api://AzureADTokenExchange"]
  issuer         = "https://token.actions.githubusercontent.com"
  subject        = "repo:${var.github_org}/${var.github_repo}:pull_request"
}

resource "azuread_application_federated_identity_credential" "main_branch" {
  application_id = azuread_application.gh_oidc.id
  display_name   = "github-main"
  description    = "Pushes to main branch (terraform apply + bundle deploy)"
  audiences      = ["api://AzureADTokenExchange"]
  issuer         = "https://token.actions.githubusercontent.com"
  subject        = "repo:${var.github_org}/${var.github_repo}:ref:refs/heads/main"
}

resource "azuread_application_federated_identity_credential" "production_env" {
  application_id = azuread_application.gh_oidc.id
  display_name   = "github-env-production"
  description    = "Reserved for environment-gated production workflows (future)"
  audiences      = ["api://AzureADTokenExchange"]
  issuer         = "https://token.actions.githubusercontent.com"
  subject        = "repo:${var.github_org}/${var.github_repo}:environment:production"
}

# ----------------------------------------------------------------------------
# RBAC role assignments
# ----------------------------------------------------------------------------
# Contributor on the subscription scope. This is broad on purpose — the SP
# needs to manage everything under the gridsense resource group(s), and a
# single subscription-scoped assignment is simpler to reason about than 10+
# resource-group-scoped ones. Tighten later if a security review demands it.

resource "azurerm_role_assignment" "subscription_contributor" {
  scope                = data.azurerm_subscription.current.id
  role_definition_name = "Contributor"
  principal_id         = azuread_service_principal.gh_oidc.object_id
}

# Storage Blob Data Contributor on the tfstate storage account.
# Contributor on the subscription gives mgmt-plane access (can change SKU,
# delete the account, etc.). It does NOT grant data-plane access (read/write
# blobs inside the container) — that requires a separate role.

resource "azurerm_role_assignment" "tfstate_blob_data_contributor" {
  scope                = var.tfstate_storage_account_id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azuread_service_principal.gh_oidc.object_id
}

# ----------------------------------------------------------------------------
# Key Vault data-plane access
# ----------------------------------------------------------------------------
# Subscription Contributor lets the SP change KV SKU / delete the vault
# (mgmt plane). It does NOT let the SP read/write secrets (data plane).
# Key Vault Administrator covers full data-plane CRUD.

resource "azurerm_role_assignment" "key_vault_administrator" {
  scope                = var.key_vault_id
  role_definition_name = "Key Vault Administrator"
  principal_id         = azuread_service_principal.gh_oidc.object_id
}

# ----------------------------------------------------------------------------
# Azure AD Application read/write via Microsoft Graph
# ----------------------------------------------------------------------------
# Required for Terraform to refresh state of existing azuread_application
# resources (e.g., foundation module's "databricks_eh" app). Without this,
# every "terraform plan" fails with Authorization_RequestDenied on
# ApplicationsClient.BaseClient.Get().
#
# We grant Application.ReadWrite.OwnedBy (least privilege — covers apps
# this SP owns or will create). For broader org-wide app management you'd
# use Application.ReadWrite.All.

data "azuread_application_published_app_ids" "well_known" {}

data "azuread_service_principal" "msgraph" {
  client_id = data.azuread_application_published_app_ids.well_known.result["MicrosoftGraph"]
}

resource "azuread_app_role_assignment" "msgraph_application_readwrite_ownedby" {
  app_role_id         = data.azuread_service_principal.msgraph.app_role_ids["Application.ReadWrite.OwnedBy"]
  principal_object_id = azuread_service_principal.gh_oidc.object_id
  resource_object_id  = data.azuread_service_principal.msgraph.object_id
}
