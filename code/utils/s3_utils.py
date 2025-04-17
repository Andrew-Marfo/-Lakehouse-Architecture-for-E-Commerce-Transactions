import boto3
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('s3_utils')

s3_client = boto3.client('s3')

def move_files(bucket_name, source_path, dest_path):
    """
    Move all files from source_path to dest_path in the S3 bucket.
    """
    try:
        logger.info("Listing files in bucket %s with prefix %s", bucket_name, source_path)
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=source_path)
        if 'Contents' not in response:
            logger.warning("No files found with prefix %s", source_path)
            return

        for obj in response['Contents']:
            source_key = obj['Key']
            dest_key = source_key.replace(source_path, dest_path, 1)
            logger.info("Copying file from %s to %s in bucket %s", source_key, dest_key, bucket_name)
            s3_client.copy_object(Bucket=bucket_name, CopySource={'Bucket': bucket_name, 'Key': source_key}, Key=dest_key)
            logger.info("Deleting original file: %s", source_key)
            # s3_client.delete_object(Bucket=bucket_name, Key=source_key)
            logger.info("Successfully moved file to %s", dest_key)

    except Exception as e:
        logger.error("Error moving files from %s to %s: %s", source_path, dest_path, str(e))
        raise