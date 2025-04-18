import sys
import logging
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from delta.tables import DeltaTable
from validation import validate_dataframe
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DoubleType

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('orders_etl')

# Initialize Glue context and job
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'bucket_name'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

try:
    # Log job start
    logger.info("Starting Glue job: %s", args['JOB_NAME'])

    # Define the schema for orders
    orders_schema = StructType([
        StructField("order_num", IntegerType(), False),
        StructField("order_id", IntegerType(), False),
        StructField("user_id", IntegerType(), False),
        StructField("order_timestamp", TimestampType(), False),
        StructField("total_amount", DoubleType(), False),
        StructField("date", StringType(), False)
    ])

    # Construct S3 paths
    bucket_name = args['bucket_name']
    raw_path = f"s3://{bucket_name}/raw/orders/"
    delta_path = f"s3://{bucket_name}/lakehouse-dwh/orders/"
    rejected_path = f"s3://{bucket_name}/rejected/orders/"
    archived_path = f"s3://{bucket_name}/archived/orders/"

    # Read all CSV files in the raw/orders/ folder with the defined schema
    logger.info("Reading data from S3 path: %s", raw_path)
    orders_df = spark.read.schema(orders_schema).csv(raw_path + "*.csv", header=True)

    # Log the number of records
    record_count = orders_df.count()
    logger.info("Total number of records in the dataset: %d", record_count)

    # Validate the data
    required_columns = ["order_num", "order_id", "user_id", "order_timestamp", "total_amount", "date"]
    logger.info("Validating orders data")
    validated_df = validate_dataframe(orders_df, orders_schema, "order_id", required_columns, rejected_path)

    # Deduplicate based on order_id
    logger.info("Deduplicating data based on order_id")
    deduped_df = validated_df.dropDuplicates(["order_id"])
    deduped_count = deduped_df.count()
    logger.info("Number of records after deduplication: %d", deduped_count)

    # Write to Delta table using merge/upsert, partitioned by date
    logger.info("Writing data to Delta table: %s, partitioned by date", delta_path)
    if DeltaTable.isDeltaTable(spark, delta_path):
        logger.info("Delta table exists, performing merge/upsert")
        delta_table = DeltaTable.forPath(spark, delta_path)
        delta_table.alias("target").merge(
            deduped_df.alias("source"),
            "target.order_id = source.order_id"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    else:
        logger.info("No existing Delta table, creating new one")
        deduped_df.write.format("delta").mode("overwrite").partitionBy("date").save(delta_path)

    # Log job completion
    logger.info("Glue job completed successfully")

except Exception as e:
    logger.error("An error occurred during the Glue job: %s", str(e))
    raise

finally:
    job.commit()
    logger.info("Job committed")
    