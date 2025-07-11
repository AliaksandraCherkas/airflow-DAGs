from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.datafusion import CloudDataFusionStartPipelineOperator
from airflow.utils.dates import days_ago


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 6, 27),
    'depends_on_past': False,
    'email': ['4erkasm@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('employee_data',
          default_args=default_args,
          description='Runs an external Python script',
          schedule_interval='@daily',
          catchup=False)

with dag:
    run_script_task = BashOperator(
        task_id='run_script',
        bash_command='python /home/airflow/gcs/dags/scripts/extract.py',
    )

    
    start_pipeline = CloudDataFusionStartPipelineOperator(
    location='us-central1',
    pipeline_name='etl-pipeline',
    instance_name='fusion-demo',
    pipeline_timeout=1000,
    task_id="start_datafusion_pipeline",
)
    
    run_script_task >> start_pipeline
