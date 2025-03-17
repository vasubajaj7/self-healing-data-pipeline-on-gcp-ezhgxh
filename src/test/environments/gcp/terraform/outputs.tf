# Outputs for the GCP test environment
# This file consolidates outputs from all test environment modules
# These outputs are used by test scripts to reference the provisioned resources

output "test_environment_id" {
  description = "The unique identifier for this test environment"
  value       = ${test_environment_id}
}

output "test_environment_expiry" {
  description = "The expiry time of the test environment if auto-destroy is enabled"
  value       = ${test_environment_expiry}
}

output "test_service_account_email" {
  description = "The email of the service account used for test resources"
  value       = ${test_service_account_email}
}

output "test_network_details" {
  description = "Network details for the test environment"
  value       = {
    network_id = ${test_network_id}
    subnet_id  = ${test_subnet_id}
  }
}

output "test_storage_details" {
  description = "Storage details for the test environment"
  value       = {
    data_bucket     = ${test_data_bucket_name}
    staging_bucket  = ${test_staging_bucket_name}
    archive_bucket  = ${test_archive_bucket_name}
  }
}

output "test_pubsub_details" {
  description = "Pub/Sub details for the test environment"
  value       = {
    notification_topic        = ${test_data_notification_topic}
    notification_subscription = ${test_data_notification_subscription}
  }
}

output "test_bigquery_datasets" {
  description = "BigQuery dataset IDs for the test environment"
  value       = {
    main_dataset      = ${test_bigquery_dataset_id}
    quality_dataset   = ${quality_dataset_id}
    monitoring_dataset = ${monitoring_dataset_id}
  }
}

output "test_bigquery_tables" {
  description = "BigQuery table IDs for the test environment"
  value       = {
    source_data           = ${source_data_table_id}
    quality_rules         = ${quality_rules_table_id}
    quality_results       = ${quality_results_table_id}
    pipeline_metrics      = ${pipeline_metrics_table_id}
    healing_actions       = ${healing_actions_table_id}
    partitioned_test_table = ${partitioned_test_table_id}
  }
}

output "test_composer_details" {
  description = "Cloud Composer details for the test environment"
  value       = {
    environment_name  = ${test_composer_environment_name}
    airflow_uri       = ${test_composer_airflow_uri}
    dag_gcs_prefix    = ${test_composer_dag_gcs_prefix}
    gcs_bucket        = ${test_composer_gcs_bucket}
  }
}

output "test_environment_full_details" {
  description = "Comprehensive details about all test environment resources"
  value       = {
    environment_id = ${test_environment_id}
    expiry         = ${test_environment_expiry}
    service_account = ${test_service_account_email}
    network        = {
      network_id = ${test_network_id}
      subnet_id  = ${test_subnet_id}
    }
    storage        = {
      data_bucket     = ${test_data_bucket_name}
      staging_bucket  = ${test_staging_bucket_name}
      archive_bucket  = ${test_archive_bucket_name}
    }
    pubsub         = {
      notification_topic        = ${test_data_notification_topic}
      notification_subscription = ${test_data_notification_subscription}
    }
    bigquery       = {
      datasets = {
        main_dataset      = ${test_bigquery_dataset_id}
        quality_dataset   = ${quality_dataset_id}
        monitoring_dataset = ${monitoring_dataset_id}
      }
      tables = {
        source_data           = ${source_data_table_id}
        quality_rules         = ${quality_rules_table_id}
        quality_results       = ${quality_results_table_id}
        pipeline_metrics      = ${pipeline_metrics_table_id}
        healing_actions       = ${healing_actions_table_id}
        partitioned_test_table = ${partitioned_test_table_id}
      }
    }
    composer       = {
      environment_name  = ${test_composer_environment_name}
      airflow_uri       = ${test_composer_airflow_uri}
      dag_gcs_prefix    = ${test_composer_dag_gcs_prefix}
      gcs_bucket        = ${test_composer_gcs_bucket}
    }
  }
}