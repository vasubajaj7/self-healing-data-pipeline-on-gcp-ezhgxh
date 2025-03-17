#!/bin/bash
# setup-monitoring.sh
#
# This script automates the setup and configuration of monitoring infrastructure
# for the self-healing data pipeline, including Cloud Monitoring resources,
# alert policies, notification channels, and custom dashboards.
#
# Dependencies:
# - Google Cloud SDK (gcloud): latest
# - jq: latest

# Exit immediately if a command exits with a non-zero status
set -e

# Global variables
PROJECT_ID=""
REGION="us-central1"  # Default region
MONITORING_WORKSPACE="self-healing-pipeline-monitoring"
NOTIFICATION_CHANNELS=()
DASHBOARD_CONFIG_DIR="../config/dashboards"
ALERT_POLICY_DIR="../config/alerts"

# Color codes for output formatting
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Print usage information
usage() {
  echo "Usage: $0 [options]"
  echo ""
  echo "Options:"
  echo "  -p, --project-id PROJECT_ID    GCP Project ID (required)"
  echo "  -r, --region REGION            GCP Region (default: us-central1)"
  echo "  -d, --dashboard-dir DIR        Directory containing dashboard configurations"
  echo "  -a, --alert-dir DIR            Directory containing alert policy configurations"
  echo "  -h, --help                     Show this help message"
  echo ""
  exit 1
}

# Log messages with different severity levels
log_info() {
  echo -e "[${BLUE}INFO${NC}] $1"
}

log_success() {
  echo -e "[${GREEN}SUCCESS${NC}] $1"
}

log_warning() {
  echo -e "[${YELLOW}WARNING${NC}] $1"
}

log_error() {
  echo -e "[${RED}ERROR${NC}] $1"
}

# Check if required dependencies are installed
check_dependencies() {
  log_info "Checking dependencies..."
  
  # Check for gcloud
  if ! command -v gcloud &> /dev/null; then
    log_error "Google Cloud SDK (gcloud) is not installed or not in PATH"
    log_info "Please install from: https://cloud.google.com/sdk/docs/install"
    return 1
  else
    local gcloud_version=$(gcloud --version | head -n 1)
    log_info "Found $gcloud_version"
  fi
  
  # Check for jq
  if ! command -v jq &> /dev/null; then
    log_error "jq is not installed or not in PATH"
    log_info "Please install jq. On most systems, you can use: apt-get install jq or brew install jq"
    return 1
  else
    local jq_version=$(jq --version)
    log_info "Found $jq_version"
  fi
  
  log_success "All dependencies are installed"
  return 0
}

# Verify GCP authentication and project access
verify_gcp_access() {
  log_info "Verifying GCP access for project: $PROJECT_ID"
  
  if ! gcloud projects describe "$PROJECT_ID" &> /dev/null; then
    log_error "Unable to access project $PROJECT_ID. Please check authentication and project ID"
    return 1
  fi
  
  # Verify that the Monitoring API is enabled
  if ! gcloud services list --project "$PROJECT_ID" | grep -q monitoring.googleapis.com; then
    log_warning "Monitoring API is not enabled. Enabling now..."
    gcloud services enable monitoring.googleapis.com --project "$PROJECT_ID"
  fi
  
  log_success "GCP access verified for project: $PROJECT_ID"
  return 0
}

# Create notification channels for alerts (Email, Microsoft Teams)
setup_notification_channels() {
  log_info "Setting up notification channels..."
  
  # Create Email notification channel
  log_info "Creating Email notification channel..."
  local email_channel_config=$(mktemp)
  
  cat > "$email_channel_config" << EOF
{
  "type": "email",
  "displayName": "Pipeline Alerts - Email",
  "description": "Email notification channel for pipeline alerts",
  "labels": {
    "email_address": "pipeline-alerts@example.com"
  }
}
EOF
  
  local email_channel=$(gcloud monitoring channels create \
    --project="$PROJECT_ID" \
    --channel-content-from-file="$email_channel_config" \
    --format="value(name)")
  
  if [ -n "$email_channel" ]; then
    NOTIFICATION_CHANNELS+=("$email_channel")
    log_success "Created Email notification channel: $email_channel"
  else
    log_error "Failed to create Email notification channel"
  fi
  
  rm "$email_channel_config"
  
  # Create Microsoft Teams webhook notification channel
  log_info "Creating Microsoft Teams notification channel..."
  local teams_channel_config=$(mktemp)
  
  cat > "$teams_channel_config" << EOF
{
  "type": "webhook_tokenauth",
  "displayName": "Pipeline Alerts - Microsoft Teams",
  "description": "Microsoft Teams webhook for pipeline alerts",
  "labels": {
    "url": "https://example.webhook.office.com/webhookb2/your-teams-webhook-url",
    "auth_token": "your-auth-token"
  }
}
EOF
  
  local teams_channel=$(gcloud monitoring channels create \
    --project="$PROJECT_ID" \
    --channel-content-from-file="$teams_channel_config" \
    --format="value(name)")
  
  if [ -n "$teams_channel" ]; then
    NOTIFICATION_CHANNELS+=("$teams_channel")
    log_success "Created Microsoft Teams notification channel: $teams_channel"
  else
    log_error "Failed to create Microsoft Teams notification channel"
  fi
  
  rm "$teams_channel_config"
  
  # Return success if at least one channel was created
  if [ ${#NOTIFICATION_CHANNELS[@]} -gt 0 ]; then
    log_success "Notification channels setup complete. Created ${#NOTIFICATION_CHANNELS[@]} channels."
    return 0
  else
    log_error "Failed to create any notification channels"
    return 1
  fi
}

# Create alert policies from JSON configuration files
create_alert_policies() {
  local notification_channels=$1
  
  log_info "Creating alert policies from directory: $ALERT_POLICY_DIR"
  
  # Check if the directory exists
  if [ ! -d "$ALERT_POLICY_DIR" ]; then
    log_error "Alert policy directory does not exist: $ALERT_POLICY_DIR"
    return 1
  fi
  
  # Count the number of successful creations
  local success_count=0
  local total_files=$(find "$ALERT_POLICY_DIR" -name "*.json" | wc -l)
  
  # Process each JSON file in the directory
  for policy_file in "$ALERT_POLICY_DIR"/*.json; do
    if [ ! -f "$policy_file" ]; then
      continue
    fi
    
    local policy_name=$(basename "$policy_file" .json)
    log_info "Processing alert policy: $policy_name"
    
    # Create a temporary file for the modified policy
    local temp_policy_file=$(mktemp)
    
    # Add notification channels to the policy
    jq --arg channels "$notification_channels" \
      '.notificationChannels = ($channels | split(","))' \
      "$policy_file" > "$temp_policy_file"
    
    # Create the alert policy
    if gcloud monitoring policies create \
      --project="$PROJECT_ID" \
      --policy-from-file="$temp_policy_file"; then
      log_success "Created alert policy: $policy_name"
      ((success_count++))
    else
      log_error "Failed to create alert policy: $policy_name"
    fi
    
    rm "$temp_policy_file"
  done
  
  log_info "Alert policy creation complete. Created $success_count of $total_files policies."
  
  if [ "$success_count" -eq "$total_files" ]; then
    return 0
  else
    return 1
  fi
}

# Create monitoring dashboards from JSON configuration files
create_dashboards() {
  log_info "Creating monitoring dashboards from directory: $DASHBOARD_CONFIG_DIR"
  
  # Check if the directory exists
  if [ ! -d "$DASHBOARD_CONFIG_DIR" ]; then
    log_error "Dashboard configuration directory does not exist: $DASHBOARD_CONFIG_DIR"
    return 1
  fi
  
  # Count the number of successful creations
  local success_count=0
  local total_files=$(find "$DASHBOARD_CONFIG_DIR" -name "*.json" | wc -l)
  
  # Process each JSON file in the directory
  for dashboard_file in "$DASHBOARD_CONFIG_DIR"/*.json; do
    if [ ! -f "$dashboard_file" ]; then
      continue
    fi
    
    local dashboard_name=$(basename "$dashboard_file" .json)
    log_info "Processing dashboard: $dashboard_name"
    
    # Update the dashboard configuration with the correct project ID
    local temp_dashboard_file=$(mktemp)
    jq --arg project_id "$PROJECT_ID" \
      'walk(if type == "object" and has("dataSets") then .dataSets[].projectId = $project_id else . end)' \
      "$dashboard_file" > "$temp_dashboard_file"
    
    # Create the dashboard
    if gcloud monitoring dashboards create \
      --project="$PROJECT_ID" \
      --config-from-file="$temp_dashboard_file"; then
      log_success "Created dashboard: $dashboard_name"
      ((success_count++))
    else
      log_error "Failed to create dashboard: $dashboard_name"
    fi
    
    rm "$temp_dashboard_file"
  done
  
  log_info "Dashboard creation complete. Created $success_count of $total_files dashboards."
  
  if [ "$success_count" -eq "$total_files" ]; then
    return 0
  else
    return 1
  fi
}

# Configure uptime checks for critical services
setup_uptime_checks() {
  log_info "Setting up uptime checks for critical services..."
  
  # Define the services to monitor
  local services=(
    "api-gateway:https://api-gateway-service-url/health"
    "web-ui:https://web-ui-service-url/health"
    "pipeline-orchestrator:https://composer-webserver-url/health"
  )
  
  local success_count=0
  
  for service_info in "${services[@]}"; do
    # Split the service info into name and URL
    IFS=':' read -r service_name service_url <<< "$service_info"
    
    log_info "Creating uptime check for $service_name..."
    
    # Create a temporary file for the uptime check configuration
    local uptime_check_config=$(mktemp)
    
    cat > "$uptime_check_config" << EOF
{
  "displayName": "Uptime Check - $service_name",
  "http": {
    "path": "/health",
    "port": 443,
    "useSsl": true,
    "validateSsl": true
  },
  "monitoredResource": {
    "type": "uptime_url",
    "labels": {
      "host": "$(echo "$service_url" | sed 's|^https://||' | sed 's|/.*$||')",
      "project_id": "$PROJECT_ID"
    }
  },
  "period": "60s",
  "timeout": "10s"
}
EOF
    
    # Create the uptime check
    if uptime_check=$(gcloud monitoring uptime-check-configs create \
      --project="$PROJECT_ID" \
      --config-from-file="$uptime_check_config" \
      --format="value(name)"); then
      log_success "Created uptime check for $service_name: $uptime_check"
      ((success_count++))
    else
      log_error "Failed to create uptime check for $service_name"
    fi
    
    rm "$uptime_check_config"
  done
  
  log_info "Uptime checks setup complete. Created $success_count of ${#services[@]} checks."
  
  if [ "$success_count" -eq "${#services[@]}" ]; then
    return 0
  else
    return 1
  fi
}

# Create log-based metrics for error tracking and analysis
setup_log_based_metrics() {
  log_info "Setting up log-based metrics for error tracking and analysis..."
  
  # Define the log-based metrics to create
  local metrics=(
    "pipeline_errors:Pipeline execution errors:logName=\"projects/$PROJECT_ID/logs/composer-environment\" severity>=ERROR"
    "data_quality_issues:Data quality validation failures:logName=\"projects/$PROJECT_ID/logs/data-quality-service\" textPayload:\"validation failed\""
    "self_healing_actions:Self-healing correction actions:logName=\"projects/$PROJECT_ID/logs/self-healing-service\" textPayload:\"applied correction\""
  )
  
  local success_count=0
  
  for metric_info in "${metrics[@]}"; do
    # Split the metric info
    IFS=':' read -r metric_name metric_description filter <<< "$metric_info"
    
    log_info "Creating log-based metric: $metric_name..."
    
    # Create the log-based metric
    if gcloud logging metrics create "$metric_name" \
      --project="$PROJECT_ID" \
      --description="$metric_description" \
      --filter="$filter"; then
      log_success "Created log-based metric: $metric_name"
      ((success_count++))
    else
      log_error "Failed to create log-based metric: $metric_name"
    fi
  done
  
  log_info "Log-based metrics setup complete. Created $success_count of ${#metrics[@]} metrics."
  
  if [ "$success_count" -eq "${#metrics[@]}" ]; then
    return 0
  else
    return 1
  fi
}

# Configure custom metrics for pipeline-specific monitoring
setup_custom_metrics() {
  log_info "Setting up custom metrics for pipeline-specific monitoring..."
  
  # For custom metrics, we typically create them programmatically within the 
  # application code, but we can set up metric descriptors here to ensure 
  # they're properly defined and documented
  
  # Define the custom metrics to create
  local metrics=(
    "custom.googleapis.com/pipeline/health_score:GAUGE:Pipeline overall health score (0-100):1"
    "custom.googleapis.com/pipeline/data_quality_score:GAUGE:Data quality score by dataset (0-100):2"
    "custom.googleapis.com/pipeline/self_healing_success_rate:GAUGE:Self-healing success rate (%):1"
    "custom.googleapis.com/pipeline/data_freshness:GAUGE:Data freshness in minutes:2"
  )
  
  local success_count=0
  
  for metric_info in "${metrics[@]}"; do
    # Split the metric info
    IFS=':' read -r metric_type metric_kind metric_description value_type <<< "$metric_info"
    
    log_info "Creating custom metric descriptor: $metric_type..."
    
    # Create a temporary file for the metric descriptor
    local metric_descriptor_config=$(mktemp)
    
    cat > "$metric_descriptor_config" << EOF
{
  "type": "$metric_type",
  "metricKind": "$metric_kind",
  "valueType": "DOUBLE",
  "description": "$metric_description",
  "displayName": "$(echo "$metric_type" | sed 's|custom.googleapis.com/pipeline/||')",
  "labels": []
}
EOF
    
    # Create the metric descriptor
    if gcloud monitoring metrics descriptors create \
      --project="$PROJECT_ID" \
      --config-from-file="$metric_descriptor_config"; then
      log_success "Created custom metric descriptor: $metric_type"
      ((success_count++))
    else
      log_error "Failed to create custom metric descriptor: $metric_type"
    fi
    
    rm "$metric_descriptor_config"
  done
  
  log_info "Custom metrics setup complete. Created $success_count of ${#metrics[@]} metric descriptors."
  
  if [ "$success_count" -eq "${#metrics[@]}" ]; then
    return 0
  else
    return 1
  fi
}

# Main function that orchestrates the monitoring setup process
main() {
  # Parse command line arguments
  while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
      -p|--project-id)
        PROJECT_ID="$2"
        shift
        shift
        ;;
      -r|--region)
        REGION="$2"
        shift
        shift
        ;;
      -d|--dashboard-dir)
        DASHBOARD_CONFIG_DIR="$2"
        shift
        shift
        ;;
      -a|--alert-dir)
        ALERT_POLICY_DIR="$2"
        shift
        shift
        ;;
      -h|--help)
        usage
        ;;
      *)
        log_error "Unknown option: $1"
        usage
        ;;
    esac
  done
  
  # Validate required parameters
  if [ -z "$PROJECT_ID" ]; then
    log_error "Project ID is required"
    usage
  fi
  
  log_info "Starting monitoring setup for project: $PROJECT_ID in region: $REGION"
  
  # Check dependencies
  if ! check_dependencies; then
    log_error "Missing dependencies. Please install required dependencies and try again."
    exit 1
  fi
  
  # Verify GCP access
  if ! verify_gcp_access; then
    log_error "GCP access verification failed. Please check your credentials and project ID."
    exit 1
  fi
  
  # Setup notification channels
  if ! setup_notification_channels; then
    log_error "Failed to set up notification channels"
    exit 1
  fi
  
  # Create alert policies
  notification_channels_list=$(printf ",%s" "${NOTIFICATION_CHANNELS[@]}")
  notification_channels_list=${notification_channels_list:1}
  
  if ! create_alert_policies "$notification_channels_list"; then
    log_warning "Some alert policies could not be created"
  fi
  
  # Create dashboards
  if ! create_dashboards; then
    log_warning "Some dashboards could not be created"
  fi
  
  # Setup uptime checks
  if ! setup_uptime_checks; then
    log_warning "Some uptime checks could not be created"
  fi
  
  # Setup log-based metrics
  if ! setup_log_based_metrics; then
    log_warning "Some log-based metrics could not be created"
  fi
  
  # Setup custom metrics
  if ! setup_custom_metrics; then
    log_warning "Some custom metrics could not be created"
  fi
  
  log_success "Monitoring setup completed successfully!"
  log_info "Summary:"
  log_info "  - Notification Channels: ${#NOTIFICATION_CHANNELS[@]}"
  log_info "  - Alert Policies: Created from $ALERT_POLICY_DIR"
  log_info "  - Dashboards: Created from $DASHBOARD_CONFIG_DIR"
  log_info "  - Uptime Checks: Created for critical services"
  log_info "  - Log-based Metrics: Created for error tracking"
  log_info "  - Custom Metrics: Created for pipeline monitoring"
  
  log_info "To view resources, visit: https://console.cloud.google.com/monitoring?project=$PROJECT_ID"
  
  return 0
}

# Call the main function with all arguments
main "$@"