from airflow import models
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from google.cloud import pubsub_v1, storage
import json

# Constants
BUCKET_NAME = 'crashanalysisp'
TOPIC_NAME = 'projects/newzealandcrashproject/topics/gcs-file-events'
PREFIX = 'data_'

# Task: Detect new .csv files in GCS and publish their paths to Pub/Sub
def detect_and_publish(**context):
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    blobs = bucket.list_blobs(prefix=PREFIX)

    publisher = pubsub_v1.PublisherClient()

    for blob in blobs:
        if blob.name.endswith('.csv'):
            # Skip files already published
            if blob.metadata and blob.metadata.get("published") == "true":
                continue

            gcs_path = f'gs://{BUCKET_NAME}/{blob.name}'
            message = json.dumps({'gcs_path': gcs_path}).encode('utf-8')
            publisher.publish(TOPIC_NAME, message)
            print(f"Published message for: {gcs_path}")

            # Mark the file as published in its metadata
            metadata = blob.metadata or {}
            metadata["published"] = "true"
            blob.metadata = metadata
            blob.patch()

# DAG Configuration
default_args = {
    'start_date': days_ago(1),
}

with models.DAG(
    'crash_etl_dag',
    default_args=default_args,
    schedule_interval='*/10 * * * *',  # Every 10 minutes
    catchup=False,
) as dag:

    detect_new_crash_files = PythonOperator(
        task_id='detect_and_publish_crash_files',
        python_callable=detect_and_publish,
    )
