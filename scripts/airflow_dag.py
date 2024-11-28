from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'user',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    dag_id='etl_gcp_to_hdfs_pipeline',
    default_args=default_args,
    description='Pipeline ETL avec GCP, HDFS et Spark',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
) as dag:

    download_task = GCSToLocalFilesystemOperator(
        task_id='download_from_gcs',
        bucket_name='source_bucket_name',
        object_name='path/to/source_file.csv',
        filename='/tmp/raw_data.csv',
        gcp_conn_id='google_cloud_default',
    )

    upload_to_hdfs_task = BashOperator(
        task_id='upload_to_hdfs',
        bash_command='hdfs dfs -put -f /tmp/raw_data.csv /data/raw/'
    )

    process_json_task = BashOperator(
        task_id='process_json',
        bash_command='spark-submit /path/to/spark_json.py'
    )

    process_csv_task = BashOperator(
        task_id='process_csv',
        bash_command='spark-submit /path/to/spark_csv.py'
    )

    process_streaming_task = BashOperator(
        task_id='process_streaming',
        bash_command='spark-submit /path/to/spark_streaming.py'
    )

    process_logs_task = BashOperator(
        task_id='process_logs',
        bash_command='spark-submit /path/to/spark_logs.py'
    )

    upload_clean_to_gcs_task = LocalFilesystemToGCSOperator(
        task_id='upload_clean_to_gcs',
        src='/data/clean/',
        dst='path/to/destination_folder/',
        bucket_name='destination_bucket_name',
        gcp_conn_id='google_cloud_default',
    )

    download_task >> upload_to_hdfs_task >> [process_json_task, process_csv_task, process_streaming_task, process_logs_task] >> upload_clean_to_gcs_task
