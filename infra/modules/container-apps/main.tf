# ============================================================================
# Container Apps Module
# Provisions infrastructure for the producer services:
#   - Azure Container Registry (ACR) for Docker images
#   - Container Apps Environment (the host for individual apps)
#   - User-Assigned Managed Identity (UAMI) shared by all producers
#   - Role assignments granting UAMI:
#       - AcrPull on the registry
#       - Azure Event Hubs Data Sender on the Event Hubs namespace
#
# Individual Container Apps (one per producer) are NOT defined here.
# They're deployed via `az containerapp create` after Docker images are
# built and pushed. This keeps Terraform focused on durable infra and
# lets producer deployments iterate fast.
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
  acr_name  = "acr${var.project}${var.env}${var.random_suffix}"
  cae_name  = "cae-${var.project}-${var.env}"
  uami_name = "uami-${var.project}-producers-${var.env}"
}

# ----------------------------------------------------------------------------
# Azure Container Registry
# Basic SKU: ~$5/mo, no replication, no premium features (geo-replication,
# content trust, vnet). Fine for portfolio scope.
# ----------------------------------------------------------------------------
resource "azurerm_container_registry" "this" {
  name                          = local.acr_name
  resource_group_name           = var.rg_name
  location                      = var.location
  sku                           = "Basic"
  admin_enabled                 = false # use managed identity for auth, not admin user
  public_network_access_enabled = true  # tighten in prod

  tags = var.tags
}

# ----------------------------------------------------------------------------
# Container Apps Environment
# This is the shared compute fabric. Individual Container Apps attach to it.
# Consumption-only plan = pay per request/second; idle apps cost ~$0.
# ----------------------------------------------------------------------------
resource "azurerm_container_app_environment" "this" {
  name                       = local.cae_name
  resource_group_name        = var.rg_name
  location                   = var.location
  log_analytics_workspace_id = var.log_analytics_workspace_id

  tags = var.tags
}

# ----------------------------------------------------------------------------
# User-Assigned Managed Identity for all producers
# Single shared identity to simplify role grants. In stricter security
# postures you'd give each producer its own identity.
# ----------------------------------------------------------------------------
resource "azurerm_user_assigned_identity" "producers" {
  name                = local.uami_name
  resource_group_name = var.rg_name
  location            = var.location

  tags = var.tags
}

# ----------------------------------------------------------------------------
# Role assignment: UAMI -> AcrPull on the registry
# Required so Container Apps can pull images from our ACR using the
# managed identity, without storing registry credentials anywhere.
# ----------------------------------------------------------------------------
resource "azurerm_role_assignment" "producers_acr_pull" {
  scope                = azurerm_container_registry.this.id
  role_definition_name = "AcrPull"
  principal_id         = azurerm_user_assigned_identity.producers.principal_id
}

# ----------------------------------------------------------------------------
# Role assignment: UAMI -> Azure Event Hubs Data Sender on the namespace
# Required so producers can publish events to any topic in the namespace.
# Scoped to namespace, not individual hubs, so adding new topics later
# doesn't need additional grants.
# ----------------------------------------------------------------------------
resource "azurerm_role_assignment" "producers_eventhubs_sender" {
  scope                = var.eventhubs_namespace_id
  role_definition_name = "Azure Event Hubs Data Sender"
  principal_id         = azurerm_user_assigned_identity.producers.principal_id
}

# ----------------------------------------------------------------------------
# Producer Container Apps
# One Container App per entry in var.producers.
#
# Each app:
#   - Pulls its image from our ACR using the producers UAMI
#   - Runs with the same UAMI attached for Event Hubs OAuth
#   - Receives EVENTHUB_NAMESPACE, EVENTHUB_TOPIC, POLL_INTERVAL_S, and
#     AZURE_CLIENT_ID env vars so the producer code authenticates correctly
#   - Pinned to 1 replica (these are long-running pollers, autoscale would
#     just thrash and risk duplicate events)
# ----------------------------------------------------------------------------
resource "azurerm_container_app" "producer" {
  for_each = var.producers

  name                         = "ca-${each.key}-${var.env}"
  resource_group_name          = var.rg_name
  container_app_environment_id = azurerm_container_app_environment.this.id
  revision_mode                = "Single"

  identity {
    type         = "UserAssigned"
    identity_ids = [azurerm_user_assigned_identity.producers.id]
  }

  registry {
    server   = azurerm_container_registry.this.login_server
    identity = azurerm_user_assigned_identity.producers.id
  }

  template {
    min_replicas = each.value.min_replicas
    max_replicas = each.value.max_replicas

    container {
      name   = each.key
      image  = "${azurerm_container_registry.this.login_server}/${each.value.image_repo}:${each.value.image_tag}"
      cpu    = each.value.cpu
      memory = each.value.memory

      env {
        name  = "EVENTHUB_NAMESPACE"
        value = var.eventhubs_namespace_name
      }
      env {
        name  = "EVENTHUB_TOPIC"
        value = each.value.eventhub_topic
      }
      env {
        name  = "POLL_INTERVAL_S"
        value = tostring(each.value.poll_interval_s)
      }
      # Required: DefaultAzureCredential reads this to disambiguate when
      # multiple managed identities could be in play. Without it, auth
      # against Event Hubs can fail or pick the wrong identity.
      env {
        name  = "AZURE_CLIENT_ID"
        value = azurerm_user_assigned_identity.producers.client_id
      }
    }
  }

  tags = var.tags

  # The role assignments must exist before the app starts pulling images
  # or attempting to publish events.
  depends_on = [
    azurerm_role_assignment.producers_acr_pull,
    azurerm_role_assignment.producers_eventhubs_sender,
  ]
}
