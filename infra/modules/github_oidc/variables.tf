variable "env" {
  description = "Environment name (dev, prod)"
  type        = string
}

variable "project" {
  description = "Project name; used in display_name of the app reg"
  type        = string
}

variable "github_org" {
  description = "GitHub organization or user that owns the repo"
  type        = string
}

variable "github_repo" {
  description = "GitHub repository name (without org prefix)"
  type        = string
}

variable "tfstate_storage_account_id" {
  description = "Resource ID of the storage account holding tfstate; SP needs Storage Blob Data Contributor here"
  type        = string
}
