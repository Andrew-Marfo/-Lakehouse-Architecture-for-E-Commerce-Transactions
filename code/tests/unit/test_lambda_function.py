import boto3
import pytest
from moto import mock_s3
from code.lambda_function import move_files

@pytest.fixture
def s3_client():
    with mock_s3():
        s3 = boto3.client('s3', region_name='eu-west-1')
        s3.create_bucket(Bucket='ecommerce-lakehouse', CreateBucketConfiguration={'LocationConstraint': 'eu-west-1'})
        yield s3

def test_move_files(s3_client):
    # Upload test files to raw/products/
    s3_client.put_object(Bucket='ecommerce-lakehouse', Key='raw/products/test1.csv', Body='test data 1')
    s3_client.put_object(Bucket='ecommerce-lakehouse', Key='raw/products/test2.csv', Body='test data 2')

    # Move files
    count = move_files('ecommerce-lakehouse', 'raw/products/', 'archived/products/')

    # Check results
    assert count == 2
    # Verify files were copied
    archived_objects = s3_client.list_objects_v2(Bucket='ecommerce-lakehouse', Prefix='archived/products/')
    assert len(archived_objects.get('Contents', [])) == 2
    assert {obj['Key'] for obj in archived_objects['Contents']} == {'archived/products/test1.csv', 'archived/products/test2.csv'}
    # Verify source files remain (since delete is commented out)
    raw_objects = s3_client.list_objects_v2(Bucket='ecommerce-lakehouse', Prefix='raw/products/')
    assert len(raw_objects.get('Contents', [])) == 2