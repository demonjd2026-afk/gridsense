# ============================================================================
# Databricks Module - Outputs
# ============================================================================

output "workspace_id" {
  value       = azurerm_databricks_workspace.this.id
  description = "Databricks workspace resource ID"
}

output "workspace_url" {
  value       = "https://${azurerm_databricks_workspace.this.workspace_url}"
  description = "Databricks workspace URL (paste in browser to open)"
}

output "workspace_name" {
  value       = azurerm_databricks_workspace.this.name
  description = "Databricks workspace name"
}

output "managed_resource_group_id" {
  value       = azurerm_databricks_workspace.this.managed_resource_group_id
  description = "ID of the Databricks-managed resource group (contains VMs/NICs/etc)"
}
