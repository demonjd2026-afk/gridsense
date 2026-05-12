# ============================================================================
# Event Hubs Module - Outputs
# ============================================================================

output "namespace_name" {
  value       = azurerm_eventhub_namespace.this.name
  description = "Event Hubs namespace name (used in Kafka bootstrap.servers)"
}

output "namespace_id" {
  value       = azurerm_eventhub_namespace.this.id
  description = "Event Hubs namespace resource ID (for role assignments)"
}

output "kafka_bootstrap_server" {
  value       = "${azurerm_eventhub_namespace.this.name}.servicebus.windows.net:9093"
  description = "Kafka-compatible bootstrap server endpoint"
}

output "topic_names" {
  value       = [for t in azurerm_eventhub.topic : t.name]
  description = "List of topic (event hub) names"
}
