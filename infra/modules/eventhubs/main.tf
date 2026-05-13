# ============================================================================
# Event Hubs Module
# Provisions: Event Hubs namespace + 3 topics (one per data source) +
# consumer groups for Bronze streaming jobs.
# Standard tier required for Kafka surface (used by aiokafka producers).
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
  ns_name = "evhns-${var.project}-${var.env}"
  topics  = ["carbon-intensity", "entsoe", "open-meteo"]
}

# ----------------------------------------------------------------------------
# Event Hubs Namespace
# Standard tier: required for Kafka surface (aiokafka client compatibility).
# 1 TU base + auto-inflate to 2 = ~$22/mo worst case, $11/mo typical.
# ----------------------------------------------------------------------------
resource "azurerm_eventhub_namespace" "this" {
  name                = local.ns_name
  location            = var.location
  resource_group_name = var.rg_name
  sku                 = "Standard"
  capacity            = 1 # 1 throughput unit = 1MB/s ingress, 2MB/s egress

  auto_inflate_enabled     = true
  maximum_throughput_units = 2

  # Disable public local-auth in prod; allow during dev for easier testing
  local_authentication_enabled = true

  tags = var.tags
}

# ----------------------------------------------------------------------------
# One topic (event hub) per data source
# 4 partitions = parallelism cap; can scale up but not down
# 1 day retention = sufficient for replay during a typical incident window
# ----------------------------------------------------------------------------
resource "azurerm_eventhub" "topic" {
  for_each = toset(local.topics)

  name                = each.key
  namespace_name      = azurerm_eventhub_namespace.this.name
  resource_group_name = var.rg_name
  partition_count     = 4
  message_retention   = 1 # days; Standard SKU max is 7
}

# ----------------------------------------------------------------------------
# Dedicated consumer group for each Bronze streaming job
# (separate from the default $Default group so other consumers don't interfere)
# ----------------------------------------------------------------------------
resource "azurerm_eventhub_consumer_group" "bronze" {
  for_each = azurerm_eventhub.topic

  name                = "bronze-ingest"
  namespace_name      = azurerm_eventhub_namespace.this.name
  eventhub_name       = each.value.name
  resource_group_name = var.rg_name
}


# ----------------------------------------------------------------------------
# Role assignment: Databricks Access Connector -> Azure Event Hubs Data Receiver
# Required so Bronze streaming jobs in Databricks can consume from any topic
# in this namespace using the cluster's managed identity (no connection strings).
# Scoped to the namespace so adding new topics doesn't need additional grants.
# ----------------------------------------------------------------------------
resource "azurerm_role_assignment" "databricks_eventhubs_receiver" {
  scope                = azurerm_eventhub_namespace.this.id
  role_definition_name = "Azure Event Hubs Data Receiver"
  principal_id         = var.databricks_access_connector_principal_id
}


# ----------------------------------------------------------------------------
# Role assignment: Databricks-EH SP -> Azure Event Hubs Data Receiver
# Mirrors the Databricks Access Connector grant above; needed because
# Databricks Spark Kafka client uses SP OAuth rather than the cluster MI.
# ----------------------------------------------------------------------------
resource "azurerm_role_assignment" "databricks_eh_sp_receiver" {
  scope                = azurerm_eventhub_namespace.this.id
  role_definition_name = "Azure Event Hubs Data Receiver"
  principal_id         = var.databricks_eh_sp_object_id
}
