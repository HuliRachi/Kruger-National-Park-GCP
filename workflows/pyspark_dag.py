import airflow 
from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator


PROJECT_ID = "project.....huli" #change name
REGION = "africa-south1"
CLUSTER_NAME = "my_demo_cluster2" # verify if name is correct
COMPOSER_BUCKET = "africa-south.............." # composer bucket name

GCS_JOB_FILE_1 = f"gs://{COMPOSER_BUCKET}/data/INGESTION/kruger_mysqlToLanding.py"
PYSPARK_JOB_1 = {"reference": {"project_id": PROJECT_ID}, "placement":{"cluster_name":CLUSTER_NAME}, "pyspark_job":{"main_python_file_uri":GCS_JOB_FILE_1}}

GCS_JOB_FILE_2 = f"gs://{COMPOSER_BUCKET}/data/INGESTION/free_entranceToBronze.py"
PYSPARK_JOB_2 = {"reference": {"project_id": PROJECT_ID}, "placement":{"cluster_name":CLUSTER_NAME}, "pyspark_job":{"main_python_file_uri":GCS_JOB_FILE_2}}

GCS_JOB_FILE_3 = f"gs://{COMPOSER_BUCKET}/data/INGESTION/gate_codesToBronze.py"
PYSPARK_JOB_3 = {"reference": {"project_id": PROJECT_ID}, "placement":{"cluster_name":CLUSTER_NAME}, "pyspark_job":{"main_python_file_uri":GCS_JOB_FILE_3}}

ARGS = {
    "owner": "RACHI HULI",
    "start_date": None,
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id = "pyspark_dag",
    schedule_interval = None,
    default_args = ARGS,
    tags = ["pyspark", "dataproc", "etl"]
)as dag:
    
    pyspark_task_1 = DataprocSubmitJobOperator(
        task_id = "pyspark_task_1",
        job = PYSPARK_JOB_1,
        region = REGION,
        project_id = PROJECT_ID
    )
    pyspark_task_2 = DataprocSubmitJobOperator(
        task_id = "pyspark_task_2",
        job = PYSPARK_JOB_2,
        region = REGION,
        project_id = PROJECT_ID
    )
    pyspark_task_3 = DataprocSubmitJobOperator(
        task_id = "pyspark_task_3",
        job = PYSPARK_JOB_3,
        region = REGION,
        project_id = PROJECT_ID
    )

pyspark_task_1 >> pyspark_task_2 >> pyspark_task_3