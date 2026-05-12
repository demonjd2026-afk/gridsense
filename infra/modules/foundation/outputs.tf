# ============================================================================
# Foundation Module - Outputs
# Consumed by eventhubs, databricks, and container-apps modules.
# ============================================================================

output "resource_group_name" {
  value       = azurerm_resource_group.this.name
  description = "Name of the parent resource group"
}

output "location" {
  value       = azurerm_resource_group.this.location
  description = "Region the foundation is deployed in"
}

output "storage_account_name" {
  value       = azurerm_storage_account.lake.name
  description = "ADLS Gen2 storage account name (bronze/silver/gold zones live here)"
}

output "storage_account_id" {
  value       = azurerm_storage_account.lake.id
  description = "ADLS Gen2 storage account resource ID"
}

output "storage_dfs_endpoint" {
  value       = azurerm_storage_account.lake.primary_dfs_endpoint
  description = "DFS endpoint URL (use for abfss:// paths)"
}

output "key_vault_id" {
  value       = azurerm_key_vault.this.id
  description = "Key Vault resource ID"
}

output "key_vault_uri" {
  value       = azurerm_key_vault.this.vault_uri
  description = "Key Vault DNS URI"
}

output "log_analytics_workspace_id" {
  value       = azurerm_log_analytics_workspace.this.id
  description = "Log Analytics workspace resource ID"
}

output "log_analytics_workspace_customer_id" {
  value       = azurerm_log_analytics_workspace.this.workspace_id
  description = "Log Analytics workspace ID (GUID format used by agents)"
}

output "access_connector_id" {
  value       = azurerm_databricks_access_connector.this.id
  description = "Databricks Access Connector resource ID (used for Unity Catalog storage credentials)"
}

output "access_connector_principal_id" {
  value       = azurerm_databricks_access_connector.this.identity[0].principal_id
  description = "Managed identity principal ID of the access connector"
}

output "tags" {
  value       = local.tags
  description = "Common tag set applied to all foundation resources"
}
