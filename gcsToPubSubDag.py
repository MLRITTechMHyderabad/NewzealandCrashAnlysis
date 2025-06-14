from airflow import models
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from google.cloud import pubsub_v1, storage
import json
import logging

# Constants
BUCKET_NAME = 'newzealandcrash'
TOPIC_NAME = 'projects/newzealandcrash/topics/csv-file-events'
PREFIX = 'data/'

def detect_and_publish(**kwargs):
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET_NAME)
        blobs = bucket.list_blobs(prefix=PREFIX)

        publisher = pubsub_v1.PublisherClient()

        for blob in blobs:
            if blob.name.endswith('.csv'):
                # Skip files already published
                if blob.metadata and blob.metadata.get("published") == "true":
                    logging.info(f"Skipping already published file: {blob.name}")
                    continue

                gcs_path = f'gs://{BUCKET_NAME}/{blob.name}'
                message = json.dumps({'gcs_path': gcs_path}).encode('utf-8')

                # Publish message to Pub/Sub and wait for confirmation
                future = publisher.publish(TOPIC_NAME, message)
                message_id = future.result()
                logging.info(f"Published {gcs_path} with message ID: {message_id}")

                # Update GCS metadata to mark as published
                blob.metadata = blob.metadata or {}
                blob.metadata["published"] = "true"
                blob.patch()
                logging.info(f"Metadata updated for {blob.name}")

    except Exception as e:
        logging.error(f"Error during detection/publish: {str(e)}")
        raise

default_args = {
    'start_date': days_ago(1),
}

with models.DAG(
    dag_id='crash_etl_dag',
    default_args=default_args,
    schedule_interval='*/10 * * * *',  # Every 10 minutes
    catchup=False,
    tags=['crash', 'gcs', 'pubsub'],
) as dag:

    detect_new_crash_files = PythonOperator(
        task_id='detect_and_publish_crash_files',
        python_callable=detect_and_publish,
        provide_context=True
    )
