import boto3
import logging

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3')


def move_files(bucket_name, source_prefix, dest_prefix):
    """Move files from source_prefix to dest_prefix in the specified S3 bucket and return the number of files moved."""
    try:
        # List all objects in the source prefix
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=source_prefix)
        if 'Contents' not in response:
            logger.info(f"No files found in s3://{bucket_name}/{source_prefix}")
            return 0

        # Counter for moved files
        moved_files_count = 0

        # Move each file
        for obj in response['Contents']:
            source_key = obj['Key']
            # Skip if the key is just the folder itself (ends with '/')
            if source_key.endswith('/'):
                continue

            # Construct the destination key
            dest_key = source_key.replace(source_prefix, dest_prefix, 1)
            logger.info(
                f"Moving s3://{bucket_name}/{source_key} to s3://{bucket_name}/{dest_key}"
            )

            # Copy the file to the destination
            copy_source = {'Bucket': bucket_name, 'Key': source_key}
            s3_client.copy_object(Bucket=bucket_name, CopySource=copy_source, Key=dest_key)

            # Delete the source file
            # s3_client.delete_object(Bucket=bucket_name, Key=source_key)

            # Increment the counter
            moved_files_count += 1

        # Log the number of files moved for this dataset
        logger.info(
            f"Moved {moved_files_count} files from s3://{bucket_name}/{source_prefix} to "
            f"s3://{bucket_name}/{dest_prefix}"
        )
        return moved_files_count

    except Exception as e:
        logger.error(f"Error moving files from {source_prefix} to {dest_prefix}: {str(e)}")
        raise


def lambda_handler(event, context):
    """Lambda handler to archive files for products, orders, and order_items, and log the number of files moved."""
    try:
        # Get the bucket name from the event
        bucket_name = event.get('bucket_name', 'ecommerce-lakehouse')
        logger.info(f"Archiving files for bucket: {bucket_name}")

        # Define the source and destination prefixes for each dataset
        datasets = [
            ('raw/products/', 'archived/products/'),
            ('raw/orders/', 'archived/orders/'),
            ('raw/order_items/', 'archived/order_items/')
        ]

        # Counter for total files moved across all datasets
        total_files_moved = 0

        # Move files for each dataset and accumulate the count
        for source_prefix, dest_prefix in datasets:
            moved_count = move_files(bucket_name, source_prefix, dest_prefix)
            total_files_moved += moved_count

        # Log the total number of files moved
        logger.info(f"Total files moved across all datasets: {total_files_moved}")

        logger.info("Successfully archived all files")
        return {
            'statusCode': 200,
            'body': f'Files archived successfully. Total files moved: {total_files_moved}'
        }

    except Exception as e:
        logger.error(f"Error in Lambda execution: {str(e)}")
        return {
            'statusCode': 500,
            'body': f"Error: {str(e)}"
        }