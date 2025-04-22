# tests/test_product_etl.py
import pytest
from unittest.mock import patch, MagicMock
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from src.glue_scripts.product_etl import transform_product_data

@pytest.fixture
def mock_glue_context(spark_session):
    glue_context = MagicMock()
    glue_context.spark_session = spark_session
    return glue_context

@pytest.fixture
def mock_delta_table(spark_session):
    with patch('delta.tables.DeltaTable') as mock:
        delta_table = MagicMock()
        mock.forPath.return_value = delta_table
        mock.isDeltaTable.return_value = False  # Default to new table
        yield mock

def test_product_etl_full_flow(spark_session, mock_glue_context, mock_delta_table, tmp_path):
    # Setup test data
    test_data = [
        (1, 101, "Electronics", "Smartphone"),  # valid
        (1, 101, "Electronics", "Smartphone"),  # duplicate
        (2, 102, "Clothing", "T-Shirt"),       # valid
        (None, 103, "Home", "Couch")           # invalid
    ]
    df = spark_session.createDataFrame(test_data, ["product_id", "department_id", "department", "product_name"])
    
    # Mock the S3 read
    with patch('pyspark.sql.DataFrameReader.csv') as mock_read:
        mock_read.return_value = df
        
        # Mock the job initialization
        with patch('awsglue.job.Job') as mock_job:
            # Import and run the script with patched dependencies
            with patch.dict('os.environ', {'JOB_NAME': 'test_job', 'bucket_name': 'test-bucket'}):
                from src.glue_scripts.product_etl import main
                main()
    
    # Verify Delta table operations
    assert mock_delta_table.isDeltaTable.called
    assert mock_delta_table.forPath.called
    
    # Verify job commit was called
    mock_job.return_value.commit.assert_called_once()

def test_delta_table_merge(spark_session, mock_glue_context, mock_delta_table):
    # Setup test data
    test_data = [(1, 101, "Electronics", "Smartphone")]
    df = spark_session.createDataFrame(test_data, ["product_id", "department_id", "department", "product_name"])
    
    # Configure mock to simulate existing Delta table
    mock_delta_table.isDeltaTable.return_value = True
    
    with patch('pyspark.sql.DataFrameReader.csv') as mock_read:
        mock_read.return_value = df
        with patch('awsglue.job.Job'):
            with patch.dict('os.environ', {'JOB_NAME': 'test_job', 'bucket_name': 'test-bucket'}):
                from src.glue_scripts.product_etl import main
                main()
    
    # Verify merge was called
    delta_table_instance = mock_delta_table.forPath.return_value
    delta_table_instance.alias.return_value.merge.assert_called_once()