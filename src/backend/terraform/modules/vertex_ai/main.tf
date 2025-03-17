# Main Terraform configuration file for Vertex AI module
# This module creates and configures Google Cloud Vertex AI resources for the self-healing data pipeline

# Local variables for resource naming and configuration
locals {
  resource_name_prefix = "${var.resource_prefix}-${var.environment}"
  common_labels = merge(var.labels, {
    module      = "vertex-ai"
    environment = var.environment
  })
  network_config = var.enable_private_endpoints ? {
    network = var.network_name
    subnet  = var.subnet_name
  } : null
  encryption_spec = var.enable_cmek ? {
    kms_key_name = var.encryption_key_name
  } : null
  
  # Entity types and features for the feature store
  entitytypes = {
    error_patterns = {
      description = "Error patterns for self-healing classification"
      features = [
        {
          name        = "error_message"
          description = "The error message text"
          value_type  = "STRING"
        },
        {
          name        = "error_type"
          description = "The type of error"
          value_type  = "STRING"
        },
        {
          name        = "error_source"
          description = "The source component of the error"
          value_type  = "STRING"
        },
        {
          name        = "error_frequency"
          description = "How frequently this error occurs"
          value_type  = "INT64"
        }
      ]
    },
    data_quality = {
      description = "Data quality metrics for self-healing"
      features = [
        {
          name        = "dataset_id"
          description = "The dataset identifier"
          value_type  = "STRING"
        },
        {
          name        = "column_name"
          description = "The column name with quality issue"
          value_type  = "STRING"
        },
        {
          name        = "issue_type"
          description = "The type of quality issue"
          value_type  = "STRING"
        },
        {
          name        = "issue_count"
          description = "Count of quality issues"
          value_type  = "INT64"
        },
        {
          name        = "historical_values"
          description = "Historical values for this column"
          value_type  = "ARRAY"
        }
      ]
    },
    pipeline_metrics = {
      description = "Pipeline performance metrics for predictive analysis"
      features = [
        {
          name        = "pipeline_id"
          description = "The pipeline identifier"
          value_type  = "STRING"
        },
        {
          name        = "execution_time"
          description = "Execution time in seconds"
          value_type  = "DOUBLE"
        },
        {
          name        = "resource_utilization"
          description = "Resource utilization percentage"
          value_type  = "DOUBLE"
        },
        {
          name        = "failure_count"
          description = "Count of failures"
          value_type  = "INT64"
        },
        {
          name        = "timestamp"
          description = "Timestamp of the metrics"
          value_type  = "TIMESTAMP"
        }
      ]
    }
  }
}

# Create a Vertex AI Featurestore
resource "google_vertex_ai_featurestore" "main" {
  name    = "${local.resource_name_prefix}-${var.featurestore_name}"
  region  = var.region
  project = var.project_id
  
  online_serving_config {
    fixed_node_count = var.featurestore_online_serving_config.fixed_node_count
  }
  
  labels = local.common_labels
  
  encryption_spec = local.encryption_spec
  
  dynamic "network_config" {
    for_each = var.enable_private_endpoints ? [1] : []
    content {
      network = var.network_name
      subnet  = var.subnet_name
    }
  }
}

# Create Featurestore Entity Types
resource "google_vertex_ai_featurestore_entitytype" "entitytypes" {
  for_each       = local.entitytypes
  featurestore   = google_vertex_ai_featurestore.main.id
  entity_type_id = each.key
  description    = each.value.description
  labels         = local.common_labels
  
  monitoring_config {
    snapshot_analysis {
      disabled = false
      monitoring_interval = "${var.model_monitoring_config.monitoring_interval_days * 24}h"
    }
  }
  
  depends_on = [google_vertex_ai_featurestore.main]
}

# Create Features for each Entity Type
resource "google_vertex_ai_featurestore_entitytype_feature" "features" {
  for_each = flatten([
    for entitytype_key, entitytype in local.entitytypes : [
      for feature in entitytype.features : {
        entitytype_key = entitytype_key
        feature_name   = feature.name
        feature        = feature
      }
    ]
  ])
  
  entitytype  = google_vertex_ai_featurestore_entitytype.entitytypes[each.value.entitytype_key].id
  feature_id  = each.value.feature_name
  description = each.value.feature.description
  value_type  = each.value.feature.value_type
  labels      = local.common_labels
  
  depends_on = [google_vertex_ai_featurestore_entitytype.entitytypes]
}

# Create a Vertex AI Metadata Store
resource "google_vertex_ai_metadata_store" "main" {
  name            = "${local.resource_name_prefix}-${var.metadata_store_name}"
  region          = var.region
  project         = var.project_id
  description     = "Metadata store for self-healing pipeline ML artifacts"
  labels          = local.common_labels
  encryption_spec = local.encryption_spec
}

# Create a Vertex AI Tensorboard
resource "google_vertex_ai_tensorboard" "main" {
  display_name    = "${local.resource_name_prefix}-${var.tensorboard_name}"
  region          = var.region
  project         = var.project_id
  description     = "Tensorboard for visualizing self-healing model training"
  labels          = local.common_labels
  encryption_spec = local.encryption_spec
}

# Create a Vertex AI Index for error patterns
resource "google_vertex_ai_index" "error_patterns" {
  display_name       = "${local.resource_name_prefix}-error-patterns-index"
  description        = "Index for matching error patterns in self-healing"
  region             = var.region
  project            = var.project_id
  index_update_method = "BATCH_UPDATE"
  
  metadata {
    contents_delta_uri = "gs://${local.resource_name_prefix}-model-artifacts/indices/error-patterns/"
    config {
      dimensions = 768
      approximate_neighbors_count = 150
      distance_measure_type = "COSINE_DISTANCE"
      algorithm_config {
        tree_ah_config {
          leaf_node_embedding_count = 1000
          leaf_nodes_to_search_percent = 10
        }
      }
    }
  }
  
  labels          = local.common_labels
  encryption_spec = local.encryption_spec
}

# Create Vertex AI Endpoints for model serving
resource "google_vertex_ai_endpoint" "endpoints" {
  for_each       = var.endpoints
  display_name   = "${local.resource_name_prefix}-${each.value.display_name}"
  description    = "Endpoint for ${each.key} model serving"
  region         = var.region
  project        = var.project_id
  network        = var.enable_private_endpoints ? var.network_name : null
  labels         = local.common_labels
  encryption_spec = local.encryption_spec
}

# Create a Vertex AI Pipeline Job for model training
resource "google_vertex_ai_pipeline_job" "training_pipeline" {
  count        = var.enable_vertex_ai_pipelines ? 1 : 0
  display_name = "${local.resource_name_prefix}-training-pipeline"
  template_uri = "gs://google-cloud-aiplatform/pipeline-templates/text-classification/sklearn/v1/training/pipeline.json"
  location     = var.region
  project      = var.project_id
  labels       = local.common_labels
  encryption_spec = local.encryption_spec
  
  parameter_values = {
    project = var.project_id
    model_display_name = "self-healing-model"
    dataset_id = ""
    training_fraction_split = "0.8"
    validation_fraction_split = "0.1"
    test_fraction_split = "0.1"
    prediction_type = "classification"
  }
  
  service_account = var.service_account_email
  network = var.enable_private_endpoints ? var.network_name : null
}

# Create monitoring alerts for model performance
resource "google_monitoring_alert_policy" "model_monitoring_alerts" {
  count         = var.model_monitoring_config.enable ? 1 : 0
  display_name  = "${local.resource_name_prefix}-model-performance-alert"
  project       = var.project_id
  combiner      = "OR"
  
  conditions {
    display_name = "Model prediction accuracy degradation"
    
    condition_threshold {
      filter     = "resource.type = \"aiplatform.googleapis.com/Model\" AND metric.type = \"aiplatform.googleapis.com/prediction/online/accuracy\""
      duration   = "300s"
      comparison = "COMPARISON_LT"
      threshold_value = 0.8
      
      aggregations {
        alignment_period   = "300s"
        per_series_aligner = "ALIGN_MEAN"
      }
    }
  }
  
  conditions {
    display_name = "Model prediction latency increase"
    
    condition_threshold {
      filter     = "resource.type = \"aiplatform.googleapis.com/Model\" AND metric.type = \"aiplatform.googleapis.com/prediction/online/latencies\""
      duration   = "300s"
      comparison = "COMPARISON_GT"
      threshold_value = 500
      
      aggregations {
        alignment_period   = "300s"
        per_series_aligner = "ALIGN_PERCENTILE_99"
      }
    }
  }
  
  notification_channels = var.model_monitoring_config.alert_email_addresses != null ? [
    for email in var.model_monitoring_config.alert_email_addresses : google_monitoring_notification_channel.email_channels[email].name
  ] : []
  
  alert_strategy {
    auto_close = "1800s"
  }
  
  depends_on = [google_monitoring_notification_channel.email_channels]
}

# Create email notification channels for alerts
resource "google_monitoring_notification_channel" "email_channels" {
  for_each      = toset(var.model_monitoring_config.enable ? var.model_monitoring_config.alert_email_addresses : [])
  display_name  = "Email notification to ${each.key}"
  type          = "email"
  project       = var.project_id
  
  labels = {
    email_address = each.key
  }
}