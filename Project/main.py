from upload_s3 import S3Uploader
import logging
import time

def main() -> None:
    """Main function to execute the S3 upload process."""
    bucket_name = 'myawsbucket-nubank'
    local_directory = 'C://Users/Desktop/Desktop/Estudo/dataset'
    s3_prefix = 'data-StateOfData2022/'

    uploader = S3Uploader(bucket_name, s3_prefix)
    try:
        uploader.upload_directory(local_directory)
        logging.info(f"***Upload finished at {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))}***")
    except Exception as e:
        logging.error(f"***An error occurred during the upload: {e}***")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
