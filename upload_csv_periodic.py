import time
from datetime import datetime
from google.cloud import storage


bucket_name = 'nz_crash_analysis_bucket'
source_csv_path = r"D:\Project-24\python_codes\soumy_pipeline\crash_final_cleaned.csv"
upload_interval_seconds = 60  
project_id = 'nz-traffic-crash-analysis'

# data_20250101_120000.csv will be uploaded to gs://newzealandcrash/data/data_20250101_120000.csv
# Upload Function
def upload_csv(bucket_name, source_csv_path):
    client = storage.Client(project=project_id)
    bucket = client.bucket(bucket_name)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    destination_blob_name = f"data/data_{timestamp}.csv"  # 'data/' is the folder in the bucket
    
    blob = bucket.blob(destination_blob_name)
    blob.chunk_size = 256 * 1024
    blob.upload_from_filename(source_csv_path)
    print(f"Uploaded {source_csv_path} to gs://{bucket_name}/{destination_blob_name}")

if __name__ == "__main__":
    try:
        while True:
            upload_csv(bucket_name, source_csv_path)
            time.sleep(upload_interval_seconds)
    except KeyboardInterrupt:
        print("Stopped uploading.")
