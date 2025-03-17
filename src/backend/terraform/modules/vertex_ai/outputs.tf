# Outputs for the Vertex AI module
# These outputs expose resource identifiers and attributes for use by other Terraform modules

output "featurestore_id" {
  description = "The ID of the Vertex AI Featurestore created by this module"
  value       = google_vertex_ai_featurestore.main.id
}

output "featurestore_name" {
  description = "The name of the Vertex AI Featurestore created by this module"
  value       = google_vertex_ai_featurestore.main.name
}

output "entitytype_ids" {
  description = "Map of entitytype names to their IDs in the Featurestore"
  value       = {for k, v in google_vertex_ai_featurestore_entitytype.entitytypes : k => v.id}
}

output "metadata_store_id" {
  description = "The ID of the Vertex AI Metadata Store created by this module"
  value       = google_vertex_ai_metadata_store.main.id
}

output "metadata_store_name" {
  description = "The name of the Vertex AI Metadata Store created by this module"
  value       = google_vertex_ai_metadata_store.main.name
}

output "tensorboard_id" {
  description = "The ID of the Vertex AI Tensorboard created by this module"
  value       = google_vertex_ai_tensorboard.main.id
}

output "tensorboard_name" {
  description = "The display name of the Vertex AI Tensorboard created by this module"
  value       = google_vertex_ai_tensorboard.main.display_name
}

output "error_patterns_index_id" {
  description = "The ID of the Vertex AI Index for error patterns"
  value       = google_vertex_ai_index.error_patterns.id
}

output "endpoint_ids" {
  description = "Map of endpoint names to their IDs for model serving"
  value       = {for k, v in google_vertex_ai_endpoint.endpoints : k => v.id}
}

output "endpoint_names" {
  description = "Map of endpoint names to their display names for model serving"
  value       = {for k, v in google_vertex_ai_endpoint.endpoints : k => v.display_name}
}

output "training_pipeline_id" {
  description = "The ID of the Vertex AI Pipeline Job for model training, if enabled"
  value       = var.enable_vertex_ai_pipelines ? google_vertex_ai_pipeline_job.training_pipeline[0].id : null
}

output "model_monitoring_alert_policy_id" {
  description = "The ID of the monitoring alert policy for model performance, if enabled"
  value       = var.model_monitoring_config.enable ? google_monitoring_alert_policy.model_monitoring_alerts[0].id : null
}