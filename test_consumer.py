import json
import pytest
import boto3
from moto import mock_aws
# Updated imports to match your new file
from consumer import WidgetStorage, SqsPoller, S3Poller

# ----------------------------
# Sample widgets
# ----------------------------
SAMPLE_WIDGET_CREATE = {
    'type': 'create',
    'requestId': 'req-123',
    'widgetId': 'wid-123',
    'owner': 'Henry Hops',
    'description': 'Sample Widget',
    'otherAttributes': [{'name': 'size', 'value': '100'}]
}

SAMPLE_WIDGET_DELETE = {
    'type': 'delete',
    'requestId': 'req-124',
    'widgetId': 'wid-123', # Same ID
    'owner': 'Henry Hops' # Need owner for S3 key
}

# ----------------------------
# WidgetStorage S3 Tests
# ----------------------------
@mock_aws
def test_storage_s3_create():
    # Create mocked S3 client
    s3 = boto3.client("s3", region_name="us-east-1")
    bucket_name = "test-bucket"
    prefix = "widgets/"
    s3.create_bucket(Bucket=bucket_name)

    # Use WidgetStorage instead of S3Manager
    storage = WidgetStorage(
        s3_client=s3, 
        db_resource=None, 
        db_table_name=None, 
        s3_bucket_name=bucket_name, 
        s3_key_prefix=prefix
    )
    
    # Use create_or_update method
    storage.create_or_update(SAMPLE_WIDGET_CREATE)

    # S3 key format is new: "widgets/{owner-with-dashes}/{widget-id}.json"
    key = f"{prefix}{SAMPLE_WIDGET_CREATE['owner'].replace(' ', '-').lower()}/{SAMPLE_WIDGET_CREATE['widgetId']}.json"
    
    response = s3.get_object(Bucket=bucket_name, Key=key)
    content = json.loads(response['Body'].read())
    assert content['widgetId'] == SAMPLE_WIDGET_CREATE['widgetId']


# ----------------------------
# WidgetStorage DynamoDB Tests
# ----------------------------
@mock_aws
def test_storage_dynamodb_create():
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    table_name = "Widgets"
    
    # Table must be created with the correct key 'widgetId'
    table = dynamodb.create_table(
        TableName=table_name,
        KeySchema=[{'AttributeName': 'widgetId', 'KeyType': 'HASH'}],
        AttributeDefinitions=[{'AttributeName': 'widgetId', 'AttributeType': 'S'}],
        BillingMode='PAY_PER_REQUEST'
    )

    # Use WidgetStorage instead of DynamoDBManager
    storage = WidgetStorage(
        s3_client=None, 
        db_resource=dynamodb, 
        db_table_name=table_name, 
        s3_bucket_name=None, 
        s3_key_prefix=None
    )
    
    # Use create_or_update method
    storage.create_or_update(SAMPLE_WIDGET_CREATE)

    # Get item using the correct key 'widgetId'
    response = table.get_item(Key={'widgetId': SAMPLE_WIDGET_CREATE['widgetId']})
    item = response.get('Item', {})
    assert item['widgetId'] == SAMPLE_WIDGET_CREATE['widgetId']
    assert item['owner'] == 'Henry Hops'


@mock_aws
def test_storage_delete():
    # --- Setup ---
    s3 = boto3.client("s3", region_name="us-east-1")
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    
    # S3
    bucket_name = "test-bucket"
    prefix = "widgets/"
    s3.create_bucket(Bucket=bucket_name)
    
    # DynamoDB
    table_name = "Widgets"
    table = dynamodb.create_table(
        TableName=table_name,
        KeySchema=[{'AttributeName': 'widgetId', 'KeyType': 'HASH'}],
        AttributeDefinitions=[{'AttributeName': 'widgetId', 'AttributeType': 'S'}],
        BillingMode='PAY_PER_REQUEST'
    )
    
    # Create the unified storage manager
    storage = WidgetStorage(
        s3_client=s3, 
        db_resource=dynamodb, 
        db_table_name=table_name, 
        s3_bucket_name=bucket_name, 
        s3_key_prefix=prefix
    )

    # --- Pre-populate data ---
    storage.create_or_update(SAMPLE_WIDGET_CREATE)

    # --- Run Delete ---
    storage.delete(SAMPLE_WIDGET_DELETE)

    # --- Verify DynamoDB delete ---
    # The item should be gone
    response = table.get_item(Key={'widgetId': SAMPLE_WIDGET_CREATE['widgetId']})
    assert 'Item' not in response

    # --- Verify S3 delete ---
    key = f"{prefix}{SAMPLE_WIDGET_CREATE['owner'].replace(' ', '-').lower()}/{SAMPLE_WIDGET_CREATE['widgetId']}.json"
    
    # This will raise an error if the object is not found
    with pytest.raises(s3.exceptions.NoSuchKey):
        s3.get_object(Bucket=bucket_name, Key=key)


# ----------------------------
# SQS Poller Tests
# ----------------------------
@mock_aws
def test_sqs_poller():
    sqs = boto3.client("sqs", region_name="us-east-1")
    queue = sqs.create_queue(QueueName="test-queue")
    queue_url = queue['QueueUrl']

    sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(SAMPLE_WIDGET_CREATE))

    # Use SqsPoller instead of SQSConsumer
    poller = SqsPoller(sqs, queue_url)
    
    # Use poll() method
    messages = poller.poll()
    assert len(messages) == 1

    body = json.loads(messages[0]['Body'])
    assert body['widgetId'] == SAMPLE_WIDGET_CREATE['widgetId']

    # delete_message() now takes the full message dictionary
    poller.delete_message(messages[0])
    
    messages_after = poller.poll()
    assert len(messages_after) == 0


# ----------------------------
# S3 Poller Tests (New)
# ----------------------------
@mock_aws
def test_s3_poller():
    s3 = boto3.client("s3", region_name="us-east-1")
    bucket_name = "request-bucket"
    s3.create_bucket(Bucket=bucket_name)

    # Add a request object to the S3 bucket
    s3_key = "request-001.json"
    s3.put_object(
        Bucket=bucket_name,
        Key=s3_key,
        Body=json.dumps(SAMPLE_WIDGET_CREATE)
    )

    # Create the S3Poller
    poller = S3Poller(s3, bucket_name)
    
    # 1. Poll and find the message
    messages = poller.poll()
    assert len(messages) == 1
    assert messages[0]['S3Key'] == s3_key
    
    body = json.loads(messages[0]['Body'])
    assert body['widgetId'] == SAMPLE_WIDGET_CREATE['widgetId']

    # 2. Delete the message
    poller.delete_message(messages[0])
    
    # 3. Poll again and find nothing
    messages_after = poller.poll()
    assert len(messages_after) == 0