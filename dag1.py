from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator,
)
from airflow.utils.dates import days_ago
from airflow.models import Variable 

# --- CONFIGURATION ---

PROJECT_ID = 'airbnb-465211'
# PROJECT_ID = 'airbnb-465211'
REGION = 'us-central1'
GCS_BUCKET = 'my-code-bucket-airbnb'
SERVICE_ACCOUNT = "dataproc-service-acc@airbnb-465211.iam.gserviceaccount.com"


# Using Jinja templating for a unique cluster name per run to ensure idempotency.
CLUSTER_NAME = 'cluster-airbnb-dag'

PYSPARK_URI_1 = f'gs://{GCS_BUCKET}/dataproc-jobs/airbnb_raw.py'
PYSPARK_URI_2 = f'gs://{GCS_BUCKET}/dataproc-jobs/airbnb_processed.py'

# Centralized Dataproc cluster configuration
CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_size_gb": 50},
    },
    "worker_config": {
        "num_instances": 0,  # Single node cluster
        "disk_config": {"boot_disk_size_gb": 50}, 
    },
    "software_config": {
        "image_version": "2.2-debian12",
    },
    "gce_cluster_config": {
        "service_account": SERVICE_ACCOUNT
    },
}

# --- JOB DEFINITIONS ---
PYSPARK_JOB_1 = {
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": PYSPARK_URI_1},
}

PYSPARK_JOB_2 = {
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": PYSPARK_URI_2},
}

default_args = {
    'start_date': days_ago(1),
    'retries': 1,
    'project_id': PROJECT_ID,
    'region': REGION,
}

with DAG(
    'dataproc_pyspark_orchestration',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['dataproc', 'pyspark'], # Adding tags is a good practice for organization
) as dag:

    # Step 1: Create the Dataproc cluster
    create_cluster = DataprocCreateClusterOperator(
        task_id='create_dataproc_cluster',
        # project_id and region are now in default_args
        cluster_name=CLUSTER_NAME,
        cluster_config=CLUSTER_CONFIG,
    )

    # Step 2: Submit first PySpark job
    submit_pyspark_1 = DataprocSubmitJobOperator(
        task_id='submit_pyspark_job_1',
        job=PYSPARK_JOB_1,
        # project_id and region are now in default_args
    )

    # Step 3: Submit second PySpark job
    submit_pyspark_2 = DataprocSubmitJobOperator(
        task_id='submit_pyspark_job_2',
        job=PYSPARK_JOB_2,
        # project_id and region are now in default_args
    )

    # Step 4: Delete the Dataproc cluster.
    # trigger_rule='all_done' ensures this task runs even if upstream tasks fail.
    delete_cluster = DataprocDeleteClusterOperator(
        task_id='delete_dataproc_cluster',
        cluster_name=CLUSTER_NAME,
        # project_id and region are now in default_args
        trigger_rule='all_done',
    )

    # Define task dependencies
    create_cluster >> submit_pyspark_1 >> submit_pyspark_2 >> delete_cluster