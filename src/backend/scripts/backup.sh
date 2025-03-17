#!/bin/bash
# =============================================================================
# Backup Script for Self-Healing Data Pipeline
# 
# This script creates comprehensive backups of critical data assets and 
# configurations for the self-healing data pipeline, including:
# - BigQuery datasets
# - Cloud Storage buckets
# - Application configurations
#
# It supports backup rotation, integrity validation, and implements
# the data retention policies defined in the technical specifications.
# =============================================================================

# Exit on error, undefined variables, and pipe failures
set -euo pipefail

# Global variables
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
PROJECT_ROOT=$(cd "${SCRIPT_DIR}/../.." && pwd)
BACKEND_DIR=${PROJECT_ROOT}/src/backend
CONFIG_DIR=${BACKEND_DIR}/configs
BACKUP_DIR=${PROJECT_ROOT}/backups
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE=${SCRIPT_DIR}/backup_${TIMESTAMP}.log
DEFAULT_RETENTION_DAYS=30

# =============================================================================
# Helper Functions
# =============================================================================

# Logs a message to both console and log file
log_message() {
    local message="$1"
    local timestamp=$(date "+%Y-%m-%d %H:%M:%S")
    echo "[${timestamp}] ${message}"
    echo "[${timestamp}] ${message}" >> "${LOG_FILE}"
}

# Checks if all required tools and configurations are available
check_prerequisites() {
    local status=0
    
    # Check if gcloud CLI is installed
    if ! command -v gcloud &> /dev/null; then
        log_message "ERROR: gcloud CLI not found. Please install Google Cloud SDK."
        status=1
    fi
    
    # Check if gsutil is installed
    if ! command -v gsutil &> /dev/null; then
        log_message "ERROR: gsutil not found. Please install Google Cloud SDK."
        status=1
    fi
    
    # Check if bq command-line tool is installed
    if ! command -v bq &> /dev/null; then
        log_message "ERROR: bq tool not found. Please install Google Cloud SDK."
        status=1
    fi
    
    # Check if required environment variables are set
    if [[ -z "${GOOGLE_APPLICATION_CREDENTIALS:-}" ]]; then
        log_message "ERROR: GOOGLE_APPLICATION_CREDENTIALS environment variable not set."
        status=1
    fi
    
    # Verify GCP authentication status
    if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" &> /dev/null; then
        log_message "ERROR: Not authenticated with Google Cloud. Please run 'gcloud auth login'."
        status=1
    fi
    
    if [[ ${status} -eq 0 ]]; then
        log_message "All prerequisites check passed."
    fi
    
    return ${status}
}

# Sets up the backup environment based on provided parameters
setup_environment() {
    local environment="$1"
    local status=0
    
    # Load environment-specific configuration
    local env_config="${CONFIG_DIR}/environments/${environment}.conf"
    if [[ ! -f "${env_config}" ]]; then
        log_message "ERROR: Environment configuration file not found: ${env_config}"
        return 1
    fi
    
    # Source the environment configuration
    source "${env_config}"
    
    # Set GCP project and region if defined in environment config
    if [[ -n "${GCP_PROJECT:-}" ]]; then
        log_message "Setting GCP project to: ${GCP_PROJECT}"
        gcloud config set project "${GCP_PROJECT}"
    fi
    
    if [[ -n "${GCP_REGION:-}" ]]; then
        log_message "Setting default region to: ${GCP_REGION}"
        gcloud config set compute/region "${GCP_REGION}"
    fi
    
    # Create backup directories if they don't exist
    local backup_dir="${BACKUP_DIR}/${environment}/${TIMESTAMP}"
    mkdir -p "${backup_dir}/bigquery"
    mkdir -p "${backup_dir}/gcs"
    mkdir -p "${backup_dir}/configs"
    
    log_message "Environment setup completed for: ${environment}"
    log_message "Backup directory: ${backup_dir}"
    
    # Export backup directory for other functions
    export CURRENT_BACKUP_DIR="${backup_dir}"
    
    return ${status}
}

# Creates backups of specified BigQuery datasets
backup_bigquery_datasets() {
    local datasets="$1"
    local destination_bucket="$2"
    local status=0
    local dataset_count=0
    local failed_datasets=()
    
    log_message "Starting BigQuery datasets backup..."
    
    # Create backup directory for BigQuery
    local bq_backup_dir="${CURRENT_BACKUP_DIR}/bigquery"
    
    # Convert comma-separated list to array
    IFS=',' read -ra dataset_array <<< "${datasets}"
    
    for dataset in "${dataset_array[@]}"; do
        log_message "Processing dataset: ${dataset}"
        
        # Create dataset directory
        local dataset_dir="${bq_backup_dir}/${dataset}"
        mkdir -p "${dataset_dir}"
        
        # Export dataset schema
        if ! bq show --format=json "${dataset}" > "${dataset_dir}/${dataset}_schema.json" 2>>"${LOG_FILE}"; then
            log_message "ERROR: Failed to export schema for dataset ${dataset}"
            failed_datasets+=("${dataset}")
            continue
        fi
        
        # Get list of tables in the dataset
        local tables
        tables=$(bq ls --format=json "${dataset}" | jq -r '.[].tableReference.tableId')
        
        if [[ -z "${tables}" ]]; then
            log_message "No tables found in dataset ${dataset}"
            continue
        fi
        
        local table_count=0
        local failed_tables=()
        
        # Export each table
        for table in ${tables}; do
            log_message "  Exporting table: ${table}"
            
            # Export table schema
            if ! bq show --format=json "${dataset}.${table}" > "${dataset_dir}/${table}_schema.json" 2>>"${LOG_FILE}"; then
                log_message "ERROR: Failed to export schema for table ${dataset}.${table}"
                failed_tables+=("${table}")
                continue
            fi
            
            # Export table data to GCS
            local gcs_path="gs://${destination_bucket}/backups/${environment}/${TIMESTAMP}/bigquery/${dataset}/${table}_*.avro"
            if ! bq extract --destination_format=AVRO "${dataset}.${table}" "${gcs_path}" 2>>"${LOG_FILE}"; then
                log_message "ERROR: Failed to export data for table ${dataset}.${table}"
                failed_tables+=("${table}")
                continue
            fi
            
            # Create symlink to GCS location for reference
            echo "${gcs_path}" > "${dataset_dir}/${table}_data_location.txt"
            
            ((table_count++))
        done
        
        # Create metadata file for the dataset
        cat > "${dataset_dir}/metadata.json" << EOF
{
    "dataset": "${dataset}",
    "backup_timestamp": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")",
    "table_count": ${table_count},
    "failed_tables": [$(printf '"%s",' "${failed_tables[@]}" | sed 's/,$//')],
    "gcs_backup_location": "gs://${destination_bucket}/backups/${environment}/${TIMESTAMP}/bigquery/${dataset}"
}
EOF
        
        log_message "Dataset ${dataset} backup completed. Exported ${table_count} tables. Failed tables: ${#failed_tables[@]}"
        ((dataset_count++))
    done
    
    # Create overall metadata file
    cat > "${bq_backup_dir}/metadata.json" << EOF
{
    "backup_type": "bigquery",
    "backup_timestamp": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")",
    "environment": "${environment}",
    "dataset_count": ${dataset_count},
    "failed_datasets": [$(printf '"%s",' "${failed_datasets[@]}" | sed 's/,$//')],
    "gcs_backup_location": "gs://${destination_bucket}/backups/${environment}/${TIMESTAMP}/bigquery"
}
EOF
    
    # Copy backup directory to GCS
    log_message "Copying BigQuery backup metadata to GCS..."
    if ! gsutil -m cp -r "${bq_backup_dir}" "gs://${destination_bucket}/backups/${environment}/${TIMESTAMP}/" 2>>"${LOG_FILE}"; then
        log_message "ERROR: Failed to copy BigQuery backup metadata to GCS"
        status=1
    fi
    
    if [[ ${status} -eq 0 && ${#failed_datasets[@]} -eq 0 ]]; then
        log_message "BigQuery backup completed successfully. Backed up ${dataset_count} datasets."
    else
        log_message "BigQuery backup completed with issues. Backed up ${dataset_count} datasets. Failed datasets: ${#failed_datasets[@]}"
        status=1
    fi
    
    return ${status}
}

# Creates backups of specified GCS buckets
backup_gcs_buckets() {
    local buckets="$1"
    local destination_bucket="$2"
    local status=0
    local bucket_count=0
    local failed_buckets=()
    
    log_message "Starting GCS buckets backup..."
    
    # Create backup directory for GCS
    local gcs_backup_dir="${CURRENT_BACKUP_DIR}/gcs"
    
    # Convert comma-separated list to array
    IFS=',' read -ra bucket_array <<< "${buckets}"
    
    for bucket in "${bucket_array[@]}"; do
        log_message "Processing bucket: ${bucket}"
        
        # Create bucket directory for metadata
        local bucket_dir="${gcs_backup_dir}/${bucket}"
        mkdir -p "${bucket_dir}"
        
        # Get bucket metadata
        if ! gsutil ls -L -b "gs://${bucket}" > "${bucket_dir}/bucket_metadata.txt" 2>>"${LOG_FILE}"; then
            log_message "ERROR: Failed to get metadata for bucket ${bucket}"
            failed_buckets+=("${bucket}")
            continue
        fi
        
        # Sync bucket contents to backup location
        local backup_path="gs://${destination_bucket}/backups/${environment}/${TIMESTAMP}/gcs/${bucket}"
        log_message "  Syncing bucket contents to: ${backup_path}"
        
        if ! gsutil -m rsync -r "gs://${bucket}" "${backup_path}" 2>>"${LOG_FILE}"; then
            log_message "ERROR: Failed to sync contents for bucket ${bucket}"
            failed_buckets+=("${bucket}")
            continue
        fi
        
        # Get object count
        local object_count
        object_count=$(gsutil ls -r "gs://${bucket}" | wc -l)
        
        # Create metadata file for the bucket
        cat > "${bucket_dir}/metadata.json" << EOF
{
    "bucket": "${bucket}",
    "backup_timestamp": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")",
    "object_count": ${object_count},
    "gcs_backup_location": "${backup_path}"
}
EOF
        
        log_message "Bucket ${bucket} backup completed. Synced approximately ${object_count} objects."
        ((bucket_count++))
    done
    
    # Create overall metadata file
    cat > "${gcs_backup_dir}/metadata.json" << EOF
{
    "backup_type": "gcs",
    "backup_timestamp": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")",
    "environment": "${environment}",
    "bucket_count": ${bucket_count},
    "failed_buckets": [$(printf '"%s",' "${failed_buckets[@]}" | sed 's/,$//')],
    "gcs_backup_location": "gs://${destination_bucket}/backups/${environment}/${TIMESTAMP}/gcs"
}
EOF
    
    # Copy backup directory to GCS
    log_message "Copying GCS backup metadata to GCS..."
    if ! gsutil -m cp -r "${gcs_backup_dir}" "gs://${destination_bucket}/backups/${environment}/${TIMESTAMP}/" 2>>"${LOG_FILE}"; then
        log_message "ERROR: Failed to copy GCS backup metadata to GCS"
        status=1
    fi
    
    if [[ ${status} -eq 0 && ${#failed_buckets[@]} -eq 0 ]]; then
        log_message "GCS backup completed successfully. Backed up ${bucket_count} buckets."
    else
        log_message "GCS backup completed with issues. Backed up ${bucket_count} buckets. Failed buckets: ${#failed_buckets[@]}"
        status=1
    fi
    
    return ${status}
}

# Backs up application configuration files
backup_configurations() {
    local destination_bucket="$1"
    local status=0
    
    log_message "Starting configuration backup..."
    
    # Create backup directory for configurations
    local config_backup_dir="${CURRENT_BACKUP_DIR}/configs"
    
    # Copy configuration files
    log_message "Copying application configuration files..."
    if ! cp -r "${CONFIG_DIR}"/* "${config_backup_dir}/" 2>>"${LOG_FILE}"; then
        log_message "ERROR: Failed to copy configuration files"
        status=1
    fi
    
    # Backup Cloud Composer DAGs if available
    log_message "Backing up Cloud Composer DAGs..."
    local composer_env=${COMPOSER_ENV:-}
    local composer_location=${COMPOSER_LOCATION:-}
    
    if [[ -n "${composer_env}" && -n "${composer_location}" ]]; then
        local dags_dir="${config_backup_dir}/composer/dags"
        local plugins_dir="${config_backup_dir}/composer/plugins"
        
        mkdir -p "${dags_dir}" "${plugins_dir}"
        
        # Get DAGs bucket from Composer environment
        local dags_bucket
        dags_bucket=$(gcloud composer environments describe "${composer_env}" \
            --location="${composer_location}" \
            --format="value(config.dagGcsPrefix)" | sed 's|gs://\([^/]*\)/.*|\1|')
        
        if [[ -n "${dags_bucket}" ]]; then
            log_message "  Copying DAGs from bucket: ${dags_bucket}"
            gsutil -m cp -r "gs://${dags_bucket}/dags" "${dags_dir}/" 2>>"${LOG_FILE}" || {
                log_message "WARNING: Failed to copy DAGs from Composer environment"
            }
            
            log_message "  Copying plugins from bucket: ${dags_bucket}"
            gsutil -m cp -r "gs://${dags_bucket}/plugins" "${plugins_dir}/" 2>>"${LOG_FILE}" || {
                log_message "WARNING: Failed to copy plugins from Composer environment"
            }
        else
            log_message "WARNING: Could not determine DAGs bucket for Composer environment"
        fi
    else
        log_message "  Skipping Composer DAGs backup (environment not configured)"
    fi
    
    # Backup Terraform state files if available
    log_message "Backing up Terraform state files..."
    local terraform_dir="${PROJECT_ROOT}/terraform"
    if [[ -d "${terraform_dir}" ]]; then
        local tf_backup_dir="${config_backup_dir}/terraform"
        mkdir -p "${tf_backup_dir}"
        
        # Copy Terraform files
        find "${terraform_dir}" -name "*.tfstate*" -exec cp {} "${tf_backup_dir}/" \; 2>>"${LOG_FILE}" || {
            log_message "WARNING: Failed to copy some Terraform state files"
        }
    else
        log_message "  Skipping Terraform state backup (directory not found)"
    fi
    
    # Create metadata file
    cat > "${config_backup_dir}/metadata.json" << EOF
{
    "backup_type": "configurations",
    "backup_timestamp": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")",
    "environment": "${environment}",
    "composer_env": "${composer_env:-none}",
    "terraform_state_included": $([[ -d "${terraform_dir}" ]] && echo "true" || echo "false"),
    "gcs_backup_location": "gs://${destination_bucket}/backups/${environment}/${TIMESTAMP}/configs"
}
EOF
    
    # Copy backup directory to GCS
    log_message "Copying configuration backup to GCS..."
    if ! gsutil -m cp -r "${config_backup_dir}" "gs://${destination_bucket}/backups/${environment}/${TIMESTAMP}/" 2>>"${LOG_FILE}"; then
        log_message "ERROR: Failed to copy configuration backup to GCS"
        status=1
    fi
    
    if [[ ${status} -eq 0 ]]; then
        log_message "Configuration backup completed successfully."
    else
        log_message "Configuration backup completed with issues."
    fi
    
    return ${status}
}

# Removes backups older than the retention period
cleanup_old_backups() {
    local retention_days="$1"
    local backup_bucket="$2"
    local status=0
    
    log_message "Starting cleanup of old backups (retention: ${retention_days} days)..."
    
    # Calculate cutoff date in seconds since epoch
    local cutoff_date
    cutoff_date=$(date -d "${retention_days} days ago" +%s)
    
    # List all backup folders in the bucket
    local backup_folders
    backup_folders=$(gsutil ls "gs://${backup_bucket}/backups/${environment}/" 2>>"${LOG_FILE}" || echo "")
    
    if [[ -z "${backup_folders}" ]]; then
        log_message "No backups found for cleanup."
        return 0
    fi
    
    local deleted_count=0
    local failed_count=0
    
    for folder in ${backup_folders}; do
        # Extract timestamp from folder name
        local folder_timestamp
        folder_timestamp=$(basename "${folder}" | grep -oE '^[0-9]{8}_[0-9]{6}$' || echo "")
        
        if [[ -z "${folder_timestamp}" ]]; then
            continue
        fi
        
        # Convert folder timestamp to seconds since epoch
        local folder_date
        folder_date=$(date -d "$(echo "${folder_timestamp}" | sed 's/\([0-9]\{4\}\)\([0-9]\{2\}\)\([0-9]\{2\}\)_\([0-9]\{2\}\)\([0-9]\{2\}\)\([0-9]\{2\}\)/\1-\2-\3 \4:\5:\6/')" +%s)
        
        # Check if folder is older than cutoff date
        if [[ ${folder_date} -lt ${cutoff_date} ]]; then
            log_message "  Removing old backup: ${folder}"
            if ! gsutil -m rm -r "${folder}" 2>>"${LOG_FILE}"; then
                log_message "ERROR: Failed to remove old backup: ${folder}"
                ((failed_count++))
            else
                ((deleted_count++))
            fi
        fi
    done
    
    log_message "Cleanup completed. Removed ${deleted_count} old backups. Failed to remove ${failed_count} backups."
    
    if [[ ${failed_count} -gt 0 ]]; then
        status=1
    fi
    
    return ${status}
}

# Validates the created backup for integrity
validate_backup() {
    local backup_path="$1"
    local status=0
    
    log_message "Starting backup validation for: ${backup_path}"
    
    # Check if backup metadata files exist
    local metadata_files
    metadata_files=$(gsutil ls "${backup_path}/**/metadata.json" 2>>"${LOG_FILE}" || echo "")
    
    if [[ -z "${metadata_files}" ]]; then
        log_message "ERROR: No metadata files found in backup. Validation failed."
        return 1
    fi
    
    # Validate each backup component
    for metadata_file in ${metadata_files}; do
        local component_dir
        component_dir=$(dirname "${metadata_file}")
        local component_type
        component_type=$(basename "$(dirname "${component_dir}")")
        
        log_message "  Validating component: ${component_type}"
        
        # Download metadata file for validation
        local temp_metadata="/tmp/backup_metadata_${TIMESTAMP}.json"
        if ! gsutil cp "${metadata_file}" "${temp_metadata}" 2>>"${LOG_FILE}"; then
            log_message "ERROR: Failed to download metadata file: ${metadata_file}"
            status=1
            continue
        fi
        
        # Check backup type-specific validation
        case "${component_type}" in
            bigquery)
                # Check if datasets were backed up
                local dataset_count
                dataset_count=$(jq -r '.dataset_count // 0' "${temp_metadata}")
                local failed_datasets
                failed_datasets=$(jq -r '.failed_datasets | length // 0' "${temp_metadata}")
                
                log_message "    BigQuery: ${dataset_count} datasets backed up, ${failed_datasets} failed"
                
                if [[ ${dataset_count} -eq 0 || ${failed_datasets} -gt 0 ]]; then
                    log_message "WARNING: BigQuery backup may be incomplete"
                    status=1
                fi
                ;;
                
            gcs)
                # Check if buckets were backed up
                local bucket_count
                bucket_count=$(jq -r '.bucket_count // 0' "${temp_metadata}")
                local failed_buckets
                failed_buckets=$(jq -r '.failed_buckets | length // 0' "${temp_metadata}")
                
                log_message "    GCS: ${bucket_count} buckets backed up, ${failed_buckets} failed"
                
                if [[ ${bucket_count} -eq 0 || ${failed_buckets} -gt 0 ]]; then
                    log_message "WARNING: GCS backup may be incomplete"
                    status=1
                fi
                ;;
                
            configs)
                # Check if config files exist
                local config_files
                config_files=$(gsutil ls "${component_dir}/**" 2>>/dev/null | grep -v "metadata.json" | wc -l)
                
                log_message "    Configurations: ${config_files} files backed up"
                
                if [[ ${config_files} -eq 0 ]]; then
                    log_message "WARNING: Configuration backup may be empty"
                    status=1
                fi
                ;;
                
            *)
                log_message "    Unknown component type: ${component_type}"
                ;;
        esac
        
        # Clean up
        rm -f "${temp_metadata}"
    done
    
    # Create overall validation report
    local validation_report="${CURRENT_BACKUP_DIR}/validation_report.json"
    cat > "${validation_report}" << EOF
{
    "backup_path": "${backup_path}",
    "validation_timestamp": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")",
    "validation_status": $(if [[ ${status} -eq 0 ]]; then echo "\"success\""; else echo "\"warning\""; fi),
    "validation_details": "See backup log for details"
}
EOF
    
    # Upload validation report to GCS
    gsutil cp "${validation_report}" "${backup_path}/" 2>>"${LOG_FILE}" || {
        log_message "WARNING: Failed to upload validation report to GCS"
    }
    
    if [[ ${status} -eq 0 ]]; then
        log_message "Backup validation completed successfully."
    else
        log_message "Backup validation completed with warnings."
    fi
    
    return ${status}
}

# Main function that orchestrates the backup process
main() {
    local environment=""
    local project=""
    local backup_bucket=""
    local datasets=""
    local gcs_buckets=""
    local config_only=false
    local retention_days=${BACKUP_RETENTION_DAYS:-${DEFAULT_RETENTION_DAYS}}
    local backup_status=0
    
    log_message "Starting backup script (version 1.0.0)"
    
    # Parse command line arguments
    while [[ $# -gt 0 ]]; do
        case "$1" in
            -e|--environment)
                environment="$2"
                shift 2
                ;;
            -p|--project)
                project="$2"
                shift 2
                ;;
            -b|--bucket)
                backup_bucket="$2"
                shift 2
                ;;
            -d|--datasets)
                datasets="$2"
                shift 2
                ;;
            -g|--gcs-buckets)
                gcs_buckets="$2"
                shift 2
                ;;
            -c|--config-only)
                config_only=true
                shift
                ;;
            -r|--retention)
                retention_days="$2"
                shift 2
                ;;
            -h|--help)
                echo "Usage: $0 [options]"
                echo ""
                echo "Options:"
                echo "  -e, --environment ENV     Environment to backup (dev, staging, prod) [required]"
                echo "  -p, --project PROJECT     Google Cloud project ID"
                echo "  -b, --bucket BUCKET       Destination GCS bucket for backups"
                echo "  -d, --datasets DATASETS   Comma-separated list of BigQuery datasets to backup"
                echo "  -g, --gcs-buckets BUCKETS Comma-separated list of GCS buckets to backup"
                echo "  -c, --config-only         Backup only configuration files"
                echo "  -r, --retention DAYS      Number of days to retain backups (default: 30)"
                echo "  -h, --help                Display this help message"
                echo ""
                echo "Environment Variables:"
                echo "  GOOGLE_APPLICATION_CREDENTIALS  Path to GCP service account key file [required]"
                echo "  BACKUP_BUCKET                   Default GCS bucket for backups"
                echo "  BACKUP_RETENTION_DAYS           Default number of days to retain backups"
                exit 0
                ;;
            *)
                log_message "ERROR: Unknown option: $1"
                echo "Use -h or --help for usage information."
                exit 1
                ;;
        esac
    done
    
    # Validate required parameters
    if [[ -z "${environment}" ]]; then
        log_message "ERROR: Environment (-e, --environment) is required"
        echo "Use -h or --help for usage information."
        exit 1
    fi
    
    # Use environment variables if parameters not provided
    if [[ -z "${backup_bucket}" ]]; then
        backup_bucket=${BACKUP_BUCKET:-}
        if [[ -z "${backup_bucket}" ]]; then
            log_message "ERROR: Backup bucket not specified. Use -b option or set BACKUP_BUCKET environment variable."
            exit 1
        fi
    fi
    
    # Export environment variables
    export environment
    
    if [[ -n "${project}" ]]; then
        export GCP_PROJECT="${project}"
        log_message "Using specified GCP project: ${project}"
    fi
    
    # Check prerequisites
    if ! check_prerequisites; then
        log_message "ERROR: Prerequisites check failed. Exiting."
        exit 1
    fi
    
    # Setup environment
    if ! setup_environment "${environment}"; then
        log_message "ERROR: Failed to setup environment: ${environment}"
        exit 1
    fi
    
    # Perform backups based on parameters
    if [[ "${config_only}" = true ]]; then
        log_message "Performing configuration-only backup..."
        
        if ! backup_configurations "${backup_bucket}"; then
            log_message "WARNING: Configuration backup completed with issues."
            backup_status=1
        fi
    else
        # If no specific datasets/buckets provided, use defaults from environment config
        if [[ -z "${datasets}" ]]; then
            datasets=${DEFAULT_BQ_DATASETS:-}
            log_message "Using default BigQuery datasets: ${datasets:-none}"
        fi
        
        if [[ -z "${gcs_buckets}" ]]; then
            gcs_buckets=${DEFAULT_GCS_BUCKETS:-}
            log_message "Using default GCS buckets: ${gcs_buckets:-none}"
        fi
        
        # Perform BigQuery backup if datasets specified
        if [[ -n "${datasets}" ]]; then
            if ! backup_bigquery_datasets "${datasets}" "${backup_bucket}"; then
                log_message "WARNING: BigQuery backup completed with issues."
                backup_status=1
            fi
        else
            log_message "Skipping BigQuery backup (no datasets specified)"
        fi
        
        # Perform GCS backup if buckets specified
        if [[ -n "${gcs_buckets}" ]]; then
            if ! backup_gcs_buckets "${gcs_buckets}" "${backup_bucket}"; then
                log_message "WARNING: GCS backup completed with issues."
                backup_status=1
            fi
        else
            log_message "Skipping GCS backup (no buckets specified)"
        fi
        
        # Perform configuration backup
        if ! backup_configurations "${backup_bucket}"; then
            log_message "WARNING: Configuration backup completed with issues."
            backup_status=1
        fi
    fi
    
    # Create overall backup metadata
    local backup_metadata="${CURRENT_BACKUP_DIR}/backup_metadata.json"
    cat > "${backup_metadata}" << EOF
{
    "backup_id": "${TIMESTAMP}",
    "environment": "${environment}",
    "backup_timestamp": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")",
    "gcp_project": "${GCP_PROJECT:-unknown}",
    "backup_bucket": "${backup_bucket}",
    "backup_components": {
        "bigquery": $(if [[ -n "${datasets}" ]]; then echo "true"; else echo "false"; fi),
        "gcs": $(if [[ -n "${gcs_buckets}" ]]; then echo "true"; else echo "false"; fi),
        "configurations": true
    },
    "retention_days": ${retention_days},
    "status": $(if [[ ${backup_status} -eq 0 ]]; then echo "\"success\""; else echo "\"warning\""; fi)
}
EOF
    
    # Upload overall metadata to GCS
    gsutil cp "${backup_metadata}" "gs://${backup_bucket}/backups/${environment}/${TIMESTAMP}/" 2>>"${LOG_FILE}" || {
        log_message "WARNING: Failed to upload overall backup metadata to GCS"
        backup_status=1
    }
    
    # Validate backup
    if ! validate_backup "gs://${backup_bucket}/backups/${environment}/${TIMESTAMP}"; then
        log_message "WARNING: Backup validation completed with issues."
        backup_status=1
    fi
    
    # Cleanup old backups
    if ! cleanup_old_backups "${retention_days}" "${backup_bucket}"; then
        log_message "WARNING: Cleanup of old backups completed with issues."
        backup_status=1
    fi
    
    # Upload log file to GCS
    gsutil cp "${LOG_FILE}" "gs://${backup_bucket}/backups/${environment}/${TIMESTAMP}/" 2>/dev/null || {
        log_message "WARNING: Failed to upload log file to GCS"
    }
    
    if [[ ${backup_status} -eq 0 ]]; then
        log_message "Backup completed successfully!"
    else
        log_message "Backup completed with warnings. Check the log for details."
    fi
    
    return ${backup_status}
}

# Execute main function with all arguments
main "$@"