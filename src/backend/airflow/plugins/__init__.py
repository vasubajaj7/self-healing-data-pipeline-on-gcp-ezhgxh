"""
Initialization module for the Airflow plugins package in the self-healing data pipeline.
This module imports and exposes all custom hooks, operators, and sensors, making them available for use in Airflow DAGs. It serves as the entry point for the Airflow plugin system, enabling the custom components to be discovered and used by Apache Airflow.
"""

from airflow.plugins_manager import AirflowPlugin  # package_version: 2.5.x Access Airflow's plugin management system

from .hooks import hooks  # Import all custom hooks for data source connections
from .custom_operators import custom_operators  # Import all custom operators for data processing tasks
from .custom_sensors import custom_sensors  # Import all custom sensors for monitoring data availability


class SelfHealingPipelinePlugin(AirflowPlugin):
    """
    Airflow plugin class that registers all custom hooks, operators, and sensors with Airflow
    """

    name = "self_healing_pipeline_plugin"
    hooks = hooks
    operators = custom_operators
    sensors = custom_sensors
    description = "Registers custom components for the self-healing data pipeline"

    def __init__(self):
        """Initializes the plugin with all custom components"""
        # Set plugin name to 'self_healing_pipeline_plugin'
        # Import all hooks from the hooks package
        # Import all operators from the custom_operators package
        # Import all sensors from the custom_sensors package
        # Set plugin description
        super().__init__(
            name=self.name,
            hooks=self.hooks,
            operators=self.operators,
            sensors=self.sensors,
            description=self.description,
        )