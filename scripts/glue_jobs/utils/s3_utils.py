import boto3
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('s3_utils')

s3_client = boto3.client('s3')

def move_file(bucket, source_key, dest_key):
    """
    Move a file from source_key to dest_key in the same S3 bucket.
    """
    try:
        logger.info("Copying file from %s to %s in bucket %s", source_key, dest_key, bucket)
        s3_client.copy_object(Bucket=bucket, CopySource={'Bucket': bucket, 'Key': source_key}, Key=dest_key)
        logger.info("Deleting original file: %s", source_key)
        s3_client.delete_object(Bucket=bucket, Key=source_key)
        logger.info("Successfully moved file to %s", dest_key)
    except Exception as e:
        logger.error("Error moving file from %s to %s: %s", source_key, dest_key, str(e))
        raise