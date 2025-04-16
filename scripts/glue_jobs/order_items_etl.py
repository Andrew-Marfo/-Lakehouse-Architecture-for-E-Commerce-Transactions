import sys
import logging
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from delta.tables import DeltaTable
from utils.validation import validate_dataframe
from utils.s3_utils import move_files
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from pyspark.sql.functions import col

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('order_items_etl')

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

    # Define the schema for order_items
    order_items_schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("order_id", IntegerType(), False),
        StructField("user_id", IntegerType(), False),
        StructField("days_since_prior_order", IntegerType(), True),
        StructField("product_id", IntegerType(), False),
        StructField("add_to_cart_order", IntegerType(), False),
        StructField("reordered", IntegerType(), False),
        StructField("order_timestamp", TimestampType(), False),
        StructField("date", StringType(), False)
    ])

    # Construct S3 paths
    bucket_name = args['bucket_name']
    raw_path = f"s3://{bucket_name}/raw/order_items/"
    delta_path = f"s3://{bucket_name}/lakehouse-dwh/order_items/"
    orders_path = f"s3://{bucket_name}/lakehouse-dwh/orders/"
    products_path = f"s3://{bucket_name}/lakehouse-dwh/products/"
    rejected_path = f"s3://{bucket_name}/rejected/order_items/"
    archived_path = f"s3://{bucket_name}/archived/order_items/"

    # Read all CSV files in the raw/order_items/ folder with the defined schema
    logger.info("Reading data from S3 path: %s", raw_path)
    order_items_df = spark.read.schema(order_items_schema).csv(raw_path + "*.csv", header=True)

    # Log the number of records
    record_count = order_items_df.count()
    logger.info("Total number of records in the dataset: %d", record_count)

    # Validate the data (null checks, timestamps, schema enforcement)
    required_columns = ["id", "order_id", "user_id", "product_id", "add_to_cart_order", "reordered", "order_timestamp", "date"]
    logger.info("Validating order_items data for nulls and timestamps")
    validated_df = validate_dataframe(order_items_df, order_items_schema, "id", required_columns, rejected_path)

    # Read the orders and products Delta tables for referential integrity
    logger.info("Reading orders Delta table from: %s", orders_path)
    orders_df = spark.read.format("delta").load(orders_path)
    logger.info("Reading products Delta table from: %s", products_path)
    products_df = spark.read.format("delta").load(products_path)

    # Check referential integrity for order_id
    logger.info("Checking referential integrity for order_id in order_items")
    order_items_with_orders = validated_df.join(orders_df, "order_id", "left")
    invalid_order_ids = order_items_with_orders.filter(col("orders.order_id").isNull())
    if invalid_order_ids.count() > 0:
        logger.warning("Found %d order_items records with invalid order_id", invalid_order_ids.count())
        invalid_order_ids.select(validated_df["*"]).write.mode("append").csv(rejected_path)
    valid_order_items = order_items_with_orders.filter(col("orders.order_id").isNotNull()).select(validated_df["*"])

    # Check referential integrity for product_id
    logger.info("Checking referential integrity for product_id in order_items")
    order_items_with_products = valid_order_items.join(products_df, "product_id", "left")
    invalid_product_ids = order_items_with_products.filter(col("products.product_id").isNull())
    if invalid_product_ids.count() > 0:
        logger.warning("Found %d order_items records with invalid product_id", invalid_product_ids.count())
        invalid_product_ids.select(valid_order_items["*"]).write.mode("append").csv(rejected_path)
    valid_order_items_final = order_items_with_products.filter(col("products.product_id").isNotNull()).select(valid_order_items["*"])

    # Deduplicate based on id
    logger.info("Deduplicating data based on id")
    deduped_df = valid_order_items_final.dropDuplicates(["id"])
    deduped_count = deduped_df.count()
    logger.info("Number of records after deduplication: %d", deduped_count)

    # Write to Delta table using merge/upsert, partitioned by date
    logger.info("Writing data to Delta table: %s, partitioned by date", delta_path)
    if DeltaTable.isDeltaTable(spark, delta_path):
        logger.info("Delta table exists, performing merge/upsert")
        delta_table = DeltaTable.forPath(spark, delta_path)
        delta_table.alias("target").merge(
            deduped_df.alias("source"),
            "target.id = source.id"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    else:
        logger.info("No existing Delta table, creating new one")
        deduped_df.write.format("delta").mode("overwrite").partitionBy("date").save(delta_path)

    # Archive the raw files
    logger.info("Archiving raw files")
    move_files(bucket_name, "raw/order_items/", "archived/order_items/")

    # Log job completion
    logger.info("Glue job completed successfully")

except Exception as e:
    logger.error("An error occurred during the Glue job: %s", str(e))
    raise

finally:
    job.commit()
    logger.info("Job committed")