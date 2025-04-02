import os
from google.cloud import storage, bigquery

# Define bucket and dataset details
BUCKET_NAME = "ecommercedata1"
DATASET_ID = "ecommerceanalytics-455608.Ecommerce"

# Define local dataset path
path = "Newdata.csv"
print("Using local dataset:", path)
# Authenticate with Google Cloud (ensure you have the credentials JSON set up)
storage_client = storage.Client()
bucket = storage_client.bucket(BUCKET_NAME)
bq_client = bigquery.Client()

# Upload files to GCS bucket
def upload_to_gcs(local_file, bucket_name):
    blob = bucket.blob(os.path.basename(local_file))
    blob.upload_from_filename(local_file)
    print(f"Uploaded {local_file} to gs://{bucket_name}/{blob.name}")

upload_to_gcs(path, BUCKET_NAME)
print("All files uploaded successfully!")

# Load data into BigQuery
def load_to_bigquery(bucket_name, dataset_id, table_name):
    for blob in bucket.list_blobs():
        if blob.name == "Newdata.csv":  # Assuming CSV files
            table_id = f"{dataset_id}.{table_name}"
            uri = f"gs://{bucket_name}/{blob.name}"
            job_config = bigquery.LoadJobConfig(
                allow_jagged_rows=True,
                ignore_unknown_values=True,
                encoding='UTF-8',
                allow_quoted_newlines=True,
                source_format=bigquery.SourceFormat.CSV,
                autodetect=True,
                                preserve_ascii_control_characters=True,
                skip_leading_rows=1
            )
            load_job = bq_client.load_table_from_uri(uri, table_id, job_config=job_config)
            load_job.result()  # Wait for the job to complete
            print(f"Loaded {blob.name} into {table_id}")

# Example: Load data into a table named "ecommerce_data"
load_to_bigquery(BUCKET_NAME, DATASET_ID, "edata")
print("Data successfully loaded into BigQuery!")
