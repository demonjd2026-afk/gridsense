# ============================================================================
# Container Apps Module - Outputs
# ============================================================================

output "acr_name" {
  value       = azurerm_container_registry.this.name
  description = "Container Registry name (used in `az acr build` and image references)"
}

output "acr_login_server" {
  value       = azurerm_container_registry.this.login_server
  description = "Registry login server, e.g. acrgridsensedevabc.azurecr.io"
}

output "acr_id" {
  value       = azurerm_container_registry.this.id
  description = "Container Registry resource ID"
}

output "container_app_environment_id" {
  value       = azurerm_container_app_environment.this.id
  description = "Container Apps Environment resource ID (passed to `az containerapp create`)"
}

output "container_app_environment_name" {
  value       = azurerm_container_app_environment.this.name
  description = "Container Apps Environment name"
}

output "producers_uami_id" {
  value       = azurerm_user_assigned_identity.producers.id
  description = "User-assigned managed identity resource ID for producers"
}

output "producers_uami_client_id" {
  value       = azurerm_user_assigned_identity.producers.client_id
  description = "Client ID of the producers UAMI (used in DefaultAzureCredential)"
}

output "producers_uami_principal_id" {
  value       = azurerm_user_assigned_identity.producers.principal_id
  description = "Principal (object) ID of the UAMI"
}

output "producer_apps" {
  value = {
    for k, app in azurerm_container_app.producer : k => {
      name            = app.name
      id              = app.id
      image           = app.template[0].container[0].image
      latest_revision = app.latest_revision_name
    }
  }
  description = "Map of producer name -> deployed Container App details"
}
