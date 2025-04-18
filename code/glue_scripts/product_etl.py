import sys
import logging
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from delta.tables import DeltaTable
from validation import validate_dataframe
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('product_etl')

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

    # Define the schema for products
    products_schema = StructType([
        StructField("product_id", IntegerType(), False),
        StructField("department_id", IntegerType(), False),
        StructField("department", StringType(), False),
        StructField("product_name", StringType(), False)
    ])

    # Construct S3 paths
    bucket_name = args['bucket_name']
    raw_path = f"s3://{bucket_name}/raw/products/"
    delta_path = f"s3://{bucket_name}/lakehouse-dwh/products/"
    rejected_path = f"s3://{bucket_name}/rejected/products/"
    archived_path = f"s3://{bucket_name}/archived/products/"

    # Read all CSV files in the raw/products/ folder with the defined schema
    logger.info("Reading data from S3 path: %s", raw_path)
    products_df = spark.read.schema(products_schema).csv(raw_path + "*.csv", header=True)

    # Log the number of records
    record_count = products_df.count()
    logger.info("Total number of records in the dataset: %d", record_count)

    # Validate the data
    required_columns = ["product_id", "department_id", "department", "product_name"]
    logger.info("Validating products data")
    validated_df = validate_dataframe(products_df, products_schema, "product_id", required_columns, rejected_path)

    # Deduplicate based on product_id
    logger.info("Deduplicating data based on product_id")
    deduped_df = validated_df.dropDuplicates(["product_id"])
    deduped_count = deduped_df.count()
    logger.info("Number of records after deduplication: %d", deduped_count)

    # Write to Delta table using merge/upsert, partitioned by department_id
    logger.info("Writing data to Delta table: %s, partitioned by department_id", delta_path)
    if DeltaTable.isDeltaTable(spark, delta_path):
        logger.info("Delta table exists, performing merge/upsert")
        delta_table = DeltaTable.forPath(spark, delta_path)
        delta_table.alias("target").merge(
            deduped_df.alias("source"),
            "target.product_id = source.product_id"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    else:
        logger.info("No existing Delta table, creating new one")
        deduped_df.write.format("delta").mode("overwrite").partitionBy("department_id").save(delta_path)

    # Log job completion
    logger.info("Glue job completed successfully")

except Exception as e:
    logger.error("An error occurred during the Glue job: %s", str(e))
    raise

finally:
    job.commit()
    logger.info("Job committed")