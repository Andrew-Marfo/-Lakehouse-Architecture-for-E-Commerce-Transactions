import pytest
import boto3
from moto import mock_s3
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from src.glue_scripts.product_etl import deduplicate_data, products_schema
from src.glue_scripts.orders_etl import deduplicate_data as orders_deduplicate, orders_schema
from src.glue_scripts.order_items_etl import deduplicate_data as order_items_deduplicate, order_items_schema
from src.utils.validation import validate_dataframe


@pytest.fixture(scope="session")
def spark():
    builder = SparkSession.builder.appName("TestETLPipeline") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture
def s3_client():
    with mock_s3():
        s3 = boto3.client('s3', region_name='eu-west-1')
        s3.create_bucket(Bucket='ecommerce-lakehouse', CreateBucketConfiguration={'LocationConstraint': 'eu-west-1'})
        yield s3


def test_etl_pipeline(spark, s3_client, tmp_path):
    # Mock S3 paths
    bucket_name = 'ecommerce-lakehouse'
    raw_products = 'raw/products/test.csv'
    raw_orders = 'raw/orders/test.csv'
    raw_order_items = 'raw/order_items/test.csv'
    rejected_products = 'rejected/products/'
    rejected_orders = 'rejected/orders/'
    rejected_order_items = 'rejected/order_items/'
    delta_products = str(tmp_path / 'lakehouse-dwh/products')
    delta_orders = str(tmp_path / 'lakehouse-dwh/orders')
    delta_order_items = str(tmp_path / 'lakehouse-dwh/order_items')

    # Create sample data
    products_data = "product_id,department_id,department,product_name\n1,101,Electronics,Laptop\n1,101,Electronics,Laptop\n2,102,Books,Novel"
    orders_data = "order_num,order_id,user_id,order_timestamp,total_amount,date\n1,1001,501,2025-04-01 10:00:00,199.99,2025-04-01\n1,1001,501,2025-04-01 10:00:00,199.99,2025-04-01\n2,1002,502,2025-04-01 11:00:00,49.99,2025-04-01"
    order_items_data = "id,order_id,user_id,days_since_prior_order,product_id,add_to_cart_order,reordered,order_timestamp,date\n1,1001,501,30,1,1,0,2025-04-01 10:00:00,2025-04-01\n1,1001,501,30,1,1,0,2025-04-01 10:00:00,2025-04-01\n2,1002,502,15,2,2,1,2025-04-01 11:00:00,2025-04-01"

    # Upload sample data to mocked S3
    s3_client.put_object(Bucket=bucket_name, Key=raw_products, Body=products_data)
    s3_client.put_object(Bucket=bucket_name, Key=raw_orders, Body=orders_data)
    s3_client.put_object(Bucket=bucket_name, Key=raw_order_items, Body=order_items_data)

    # Process products
    products_df = spark.read.schema(products_schema).csv(f"s3://{bucket_name}/{raw_products}", header=True)
    validated_products = validate_dataframe(products_df, products_schema, "product_id", ["product_id", "department_id", "department", "product_name"], f"s3://{bucket_name}/{rejected_products}")
    deduped_products = deduplicate_data(validated_products, "product_id")
    deduped_products.write.format("delta").mode("overwrite").partitionBy("department_id").save(delta_products)
    assert deduped_products.count() == 2

    # Process orders
    orders_df = spark.read.schema(orders_schema).csv(f"s3://{bucket_name}/{raw_orders}", header=True)
    validated_orders = validate_dataframe(orders_df, orders_schema, "order_id", ["order_num", "order_id", "user_id", "order_timestamp", "total_amount", "date"], f"s3://{bucket_name}/{rejected_orders}")
    deduped_orders = orders_deduplicate(validated_orders, "order_id")
    deduped_orders.write.format("delta").mode("overwrite").partitionBy("date").save(delta_orders)
    assert deduped_orders.count() == 2

    # Process order_items
    order_items_df = spark.read.schema(order_items_schema).csv(f"s3://{bucket_name}/{raw_order_items}", header=True)
    validated_order_items = validate_dataframe(order_items_df, order_items_schema, "id", ["id", "order_id", "user_id", "product_id", "add_to_cart_order", "reordered", "order_timestamp", "date"], f"s3://{bucket_name}/{rejected_order_items}")
    # Mock referential integrity by assuming orders and products exist
    deduped_order_items = order_items_deduplicate(validated_order_items, "id")
    deduped_order_items.write.format("delta").mode("overwrite").partitionBy("date").save(delta_order_items)
    assert deduped_order_items.count() == 2

    # Verify Delta tables
    products_delta = spark.read.format("delta").load(delta_products)
    orders_delta = spark.read.format("delta").load(delta_orders)
    order_items_delta = spark.read.format("delta").load(delta_order_items)
    assert products_delta.count() == 2
    assert orders_delta.count() == 2
    assert order_items_delta.count() == 2