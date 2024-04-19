import os
import boto3
import logging
import time
from typing import Optional

class S3Uploader:
    """
    A class that provides methods to upload directories and files to an AWS S3 bucket.
    
    Attributes:
        bucket_name (str): The name of the bucket to which files will be uploaded.
        s3_prefix (str): The prefix (folder path) in the bucket where files will be stored.
        s3_client: The Boto3 S3 client.
    
    Methods:
        upload_directory(local_directory: str): Uploads a directory to the specified S3 bucket.
        _upload_file(local_path: str, base_local_directory: str): Uploads a single file to the S3 bucket.
    """

    def __init__(self, bucket_name: str, s3_prefix: Optional[str] = "") -> None:
        """Initializes the S3Uploader instance with the specified bucket name and prefix."""
        self.bucket_name = bucket_name
        self.s3_prefix = s3_prefix or ""
        self.s3_client = boto3.client('s3')
        logging.basicConfig(level=logging.INFO)

    def upload_directory(self, local_directory: str) -> None:
        """
        Uploads a directory and its subdirectories to the S3 bucket, maintaining the structure.
        
        Parameters:
            local_directory (str): The local directory path to upload.
        """
        start_time = time.time()
        logging.info(f"***Starting upload at {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))}***")
        
        for root, _, files in os.walk(local_directory):
            for filename in (f for f in files if f.lower().endswith('.csv')):
                local_path = os.path.join(root, filename)
                self._upload_file(local_path, local_directory)

        duration = time.time() - start_time
        logging.info(f"***All files uploaded in {duration:.2f} seconds.***")

    def _upload_file(self, local_path: str, base_local_directory: str) -> None:
        """
        Helper method to upload a single file to the S3 bucket.
        
        Parameters:
            local_path (str): The full path to the local file to be uploaded.
            base_local_directory (str): The base directory from which to calculate relative paths for S3.
        """
        relative_path = os.path.relpath(local_path, base_local_directory)
        s3_path = os.path.join(self.s3_prefix, relative_path).replace("\\", "/")
        
        try:
            file_start_time = time.time()
            self.s3_client.upload_file(local_path, self.bucket_name, s3_path)
            file_duration = time.time() - file_start_time
            logging.info(f"File {local_path} uploaded to {s3_path} in {file_duration:.2f} seconds.")
        except Exception as e:
            logging.error(f"***Failed to upload {local_path} to {s3_path}: {e}***")
            raise
