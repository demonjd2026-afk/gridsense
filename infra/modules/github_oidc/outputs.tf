output "client_id" {
  description = "Application (client) ID. Set as AZURE_CLIENT_ID secret/variable in GitHub Actions."
  value       = azuread_application.gh_oidc.client_id
}

output "service_principal_object_id" {
  description = "Object ID of the SP (for follow-up role assignments outside this module)"
  value       = azuread_service_principal.gh_oidc.object_id
}

output "app_registration_display_name" {
  description = "Display name of the app registration in Azure AD"
  value       = azuread_application.gh_oidc.display_name
}
