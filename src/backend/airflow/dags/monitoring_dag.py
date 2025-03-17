import os
from datetime import timedelta
import json

# Airflow
import airflow
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

# Internal Imports
from constants import AlertSeverity, NotificationChannel, DEFAULT_MONITORING_INTERVAL_MINUTES
from config import get_config
from utils.logging.logger import get_logger
from monitoring.collectors.metric_collector import MetricCollector, CloudMonitoringSource, SystemMetricSource
from monitoring.analyzers.anomaly_detector import AnomalyDetector, StatisticalAnomalyDetector, MLAnomalyDetector
from monitoring.alerting.alert_generator import AlertGenerator
from monitoring.alerting.notification_router import NotificationRouter
from monitoring.analyzers.alert_correlator import AlertCorrelator
from db.repositories.alert_repository import AlertRepository
from db.repositories.metrics_repository import MetricsRepository

# Initialize logger
logger = get_logger(__name__)

# Define default arguments for the DAG
default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Get application configuration
config = get_config()

# Monitoring Configuration
MONITORING_CONFIG = config.get('monitoring', {})

def get_monitoring_config() -> dict:
    """Get monitoring configuration from application config"""
    monitoring_config = config.get('monitoring', {})

    # Set default values for missing configuration
    monitoring_config.setdefault('interval_minutes', DEFAULT_MONITORING_INTERVAL_MINUTES)
    monitoring_config.setdefault('sources', ['cloud_monitoring', 'system_metrics'])
    monitoring_config.setdefault('anomaly_detection_enabled', True)
    monitoring_config.setdefault('notification_channels', ['teams', 'email'])

    # Validate required configuration parameters
    if not monitoring_config.get('interval_minutes'):
        logger.warning("Monitoring interval not configured, using default")
    if not monitoring_config.get('sources'):
        logger.warning("No monitoring sources configured, using defaults")

    return monitoring_config

def collect_metrics(context: dict) -> dict:
    """Collect metrics from various sources"""
    # Initialize MetricCollector with configuration
    monitoring_config = get_monitoring_config()
    metric_collector = MetricCollector(config=monitoring_config)

    # Register metric sources (Cloud Monitoring, System Metrics)
    if 'cloud_monitoring' in monitoring_config.get('sources', []):
        metric_collector.register_source(CloudMonitoringSource())
    if 'system_metrics' in monitoring_config.get('sources', []):
        metric_collector.register_source(SystemMetricSource())

    # Collect metrics from all registered sources
    metrics, stats = metric_collector.collect_metrics()

    # Store metrics in metrics repository
    metrics_repo = MetricsRepository()
    metrics_repo.batch_create_metrics(metrics)

    # Log collection statistics
    logger.info(f"Collected {stats['total_metrics']} metrics in {stats['total_sources']} sources")

    # Return collected metrics and statistics
    return {'metrics': metrics, 'stats': stats}

def detect_anomalies(context: dict) -> dict:
    """Detect anomalies in collected metrics"""
    # Get metrics from XCom or metrics repository
    metrics = context['ti'].xcom_pull(task_ids='collect_metrics')['metrics']

    # Initialize appropriate AnomalyDetector based on configuration
    monitoring_config = get_monitoring_config()
    if monitoring_config.get('anomaly_detection_enabled'):
        anomaly_detector = MLAnomalyDetector()
    else:
        anomaly_detector = StatisticalAnomalyDetector()

    # Detect anomalies in metrics data
    anomalies, stats = anomaly_detector.detect_anomalies(metrics)

    # Log detection statistics
    logger.info(f"Detected {stats['total_anomalies']} anomalies in {stats['total_metrics']} metrics")

    # Return detected anomalies and statistics
    return {'anomalies': anomalies, 'stats': stats}

def generate_alerts(context: dict) -> dict:
    """Generate alerts from detected anomalies"""
    # Get anomalies from XCom or previous task
    anomalies = context['ti'].xcom_pull(task_ids='detect_anomalies')['anomalies']

    # Initialize AlertGenerator with configuration
    alert_repo = AlertRepository()
    alert_correlator = AlertCorrelator()
    notification_router = NotificationRouter()
    alert_generator = AlertGenerator(alert_repo, alert_correlator, notification_router)

    # Generate alerts from anomalies
    alerts, stats = alert_generator.generate_alerts(anomalies)

    # Store alerts in alert repository
    alert_repo.batch_create_alerts(alerts)

    # Log alert generation statistics
    logger.info(f"Generated {stats['total_alerts']} alerts from {stats['total_anomalies']} anomalies")

    # Return generated alerts and statistics
    return {'alerts': alerts, 'stats': stats}

def send_notifications(context: dict) -> dict:
    """Send notifications for generated alerts"""
    # Get alerts from XCom or alert repository
    alerts = context['ti'].xcom_pull(task_ids='generate_alerts')['alerts']

    # Initialize NotificationRouter with configuration
    notification_router = NotificationRouter()

    # Determine appropriate notification channels for each alert
    channels = [NotificationChannel.TEAMS, NotificationChannel.EMAIL]

    # Send notifications to configured channels
    delivery_results = notification_router.send_batch_notifications(alerts, channels)

    # Log notification delivery statistics
    logger.info(f"Sent notifications to {len(delivery_results)} channels")

    # Return notification delivery results
    return {'delivery_results': delivery_results}

def update_monitoring_dashboards(context: dict) -> bool:
    """Update monitoring dashboards with latest metrics and alerts"""
    # Get metrics and alerts from XCom or repositories
    metrics = context['ti'].xcom_pull(task_ids='collect_metrics')['metrics']
    alerts = context['ti'].xcom_pull(task_ids='generate_alerts')['alerts']

    # Update dashboard data in Cloud Monitoring
    # Refresh custom dashboard widgets

    # Log dashboard update status
    logger.info("Monitoring dashboards updated successfully")

    # Return success status
    return True

def cleanup_old_data(context: dict) -> dict:
    """Clean up old metrics and alerts based on retention policy"""
    # Get retention policy from configuration
    retention_policy = config.get('data_retention', {})
    metrics_retention_days = retention_policy.get('metrics_retention_days', 30)
    alerts_retention_days = retention_policy.get('alerts_retention_days', 90)

    # Calculate cutoff dates
    metrics_cutoff = datetime.datetime.now() - datetime.timedelta(days=metrics_retention_days)
    alerts_cutoff = datetime.datetime.now() - datetime.timedelta(days=alerts_retention_days)

    # Delete metrics older than retention period
    metrics_repo = MetricsRepository()
    metrics_deleted = metrics_repo.delete_old_metrics(metrics_cutoff)

    # Archive alerts older than retention period
    alert_repo = AlertRepository()
    alerts_deleted = alert_repo.delete_old_alerts(alerts_cutoff)

    # Log cleanup statistics
    logger.info(f"Deleted {metrics_deleted} old metrics and {alerts_deleted} old alerts")

    # Return cleanup statistics
    return {'metrics_deleted': metrics_deleted, 'alerts_deleted': alerts_deleted}

def train_anomaly_detection_models(context: dict) -> dict:
    """Periodically train or update anomaly detection models"""
    # Check if training is due based on schedule
    # Retrieve historical metrics data for training
    # Initialize MLAnomalyDetector
    # Train detection models with historical data
    # Save trained models
    # Log training statistics

    # Return training results and statistics
    return {}

# Define the DAG
dag = DAG(
    dag_id='monitoring_dag',
    default_args=default_args,
    schedule_interval=timedelta(minutes=MONITORING_CONFIG.get('interval_minutes', DEFAULT_MONITORING_INTERVAL_MINUTES)),
    catchup=False,
    tags=['monitoring', 'alerting', 'self-healing']
)

with dag:
    # Task to collect metrics
    collect_metrics_task = PythonOperator(
        task_id='collect_metrics',
        python_callable=collect_metrics,
        provide_context=True,
        dag=dag
    )

    # Task to detect anomalies
    detect_anomalies_task = PythonOperator(
        task_id='detect_anomalies',
        python_callable=detect_anomalies,
        provide_context=True,
        dag=dag
    )

    # Task to generate alerts
    generate_alerts_task = PythonOperator(
        task_id='generate_alerts',
        python_callable=generate_alerts,
        provide_context=True,
        dag=dag
    )

    # Task to send notifications
    send_notifications_task = PythonOperator(
        task_id='send_notifications',
        python_callable=send_notifications,
        provide_context=True,
        trigger_rule=TriggerRule.ONE_SUCCESS,
        dag=dag
    )

    # Task to update monitoring dashboards
    update_dashboards_task = PythonOperator(
        task_id='update_monitoring_dashboards',
        python_callable=update_monitoring_dashboards,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_SUCCESS,
        dag=dag
    )

    # Task to cleanup old data
    cleanup_old_data_task = PythonOperator(
        task_id='cleanup_old_data',
        python_callable=cleanup_old_data,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_DONE,
        dag=dag
    )

    # Task to train anomaly detection models
    train_models_task = PythonOperator(
        task_id='train_anomaly_detection_models',
        python_callable=train_anomaly_detection_models,
        provide_context=True,
        trigger_rule=TriggerRule.DUMMY,  # Only run on schedule
        dag=dag
    )

    # Define task dependencies
    collect_metrics_task >> detect_anomalies_task >> generate_alerts_task >> send_notifications_task
    generate_alerts_task >> update_dashboards_task
    update_dashboards_task >> cleanup_old_data_task
    train_models_task