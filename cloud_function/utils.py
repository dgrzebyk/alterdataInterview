import logging

from google.cloud import storage
from pandas import DataFrame


def upload_blob(bucket_name: str, df: DataFrame, destination_blob_name: str) -> None:
    """Uploads a file to the bucket."""
    storage_client = storage.Client()
    if storage_client.get_bucket(bucket_name).exists():
        bucket = storage_client.bucket(bucket_name)
        bucket.blob(destination_blob_name).upload_from_string(df.to_csv(), 'text/csv')
    else:
        logging.error("Provided bucket name does not exist.")
