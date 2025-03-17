from importlib import import_module
import os
from typing import Dict, List

from src.backend.utils.logging.logger import get_logger  # src/backend/utils/logging/logger.py

logger = get_logger(__name__)

VERSION = "1.0.0"
DAGS_PATH = os.path.dirname(os.path.abspath(__file__))

all_dags = {}

def get_all_dags() -> Dict:
    """Collects all DAG objects from the DAG modules for Airflow discovery"""
    dags = {}

    # Add API ingestion DAG
    from .api_ingestion_dag import dag as api_dag
    dags["api_ingestion_dag"] = api_dag

    # Add GCS ingestion DAG
    from .gcs_ingestion_dag import dag as gcs_dag
    dags["gcs_ingestion_dag"] = gcs_dag

    # Add Cloud SQL ingestion DAG
    from .cloudsql_ingestion_dag import dag as cloudsql_dag
    dags["cloudsql_ingestion_dag"] = cloudsql_dag

    # Add all BigQuery validation DAGs
    from .quality_validation_dag import bigquery_validation_dags as bq_val_dags
    for dag in bq_val_dags:
        dags[dag.dag_id] = dag

    # Add all GCS validation DAGs
    from .quality_validation_dag import gcs_validation_dags as gcs_val_dags
    for dag in gcs_val_dags:
        dags[dag.dag_id] = dag

    # Add self-healing DAGs
    from .self_healing_dag import dag as self_healing_dag
    dags["self_healing_dag"] = self_healing_dag
    from .self_healing_dag import data_quality_healing_dags as dq_healing_dags
    for dag in dq_healing_dags:
        dags[dag.dag_id] = dag
    from .self_healing_dag import pipeline_healing_dags as pipe_healing_dags
    for dag in pipe_healing_dags:
        dags[dag.dag_id] = dag
    from .self_healing_dag import resource_healing_dags as res_healing_dags
    for dag in res_healing_dags:
        dags[dag.dag_id] = dag
    from .self_healing_dag import predictive_healing_dag as pred_healing_dag
    dags["predictive_healing_dag"] = pred_healing_dag

    # Add data processing DAG if available
    try:
        from .data_processing_dag import dag as data_processing_dag
        dags["data_processing_dag"] = data_processing_dag
    except ImportError:
        pass

    # Add monitoring DAG to the dictionary if available
    try:
        from .monitoring_dag import dag as monitoring_dag
        dags["monitoring_dag"] = monitoring_dag
    except ImportError:
        pass

    # Add optimization DAG to the dictionary if available
    try:
        from .optimization_dag import dag as optimization_dag
        dags["optimization_dag"] = optimization_dag
    except ImportError:
        pass

    logger.info(f"Collected {len(dags)} DAGs")
    return dags


def get_dag_info(dag_id: str) -> Dict:
    """Returns information about a specific DAG"""
    if dag_id in all_dags:
        dag = all_dags[dag_id]
        info = {
            "schedule": dag.schedule_interval,
            "owner": dag.default_args.get("owner"),
            "tags": dag.tags,
            "description": dag.description,
        }
        return info
    else:
        logger.warning(f"DAG ID not found: {dag_id}")
        return {}


def list_dag_ids() -> List:
    """Returns a list of all available DAG IDs"""
    return list(all_dags.keys())


def import_dag_modules() -> List:
    """Dynamically imports all DAG modules in the dags directory"""
    modules = []
    for file in os.listdir(DAGS_PATH):
        if file.startswith("__") or not file.endswith(".py"):
            continue
        module_name = file[:-3]  # Remove .py extension
        try:
            module = import_module(f".{module_name}", package=__package__)
            modules.append(module)
        except ImportError as e:
            logger.error(f"Error importing module {module_name}: {e}")
    return modules
# src/backend/airflow/dags/__init__.py