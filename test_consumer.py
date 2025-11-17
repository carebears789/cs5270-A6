import json
import pytest
import boto3
from moto import mock_aws
from consumer import S3Manager, DynamoDBManager, SQSConsumer, WidgetProcessor

# ----------------------------
# Sample widget
# ----------------------------
SAMPLE_WIDGET = {
    'type': 'create',
    'requestId': 'req-123',
    'widgetId': 'wid-123',
    'owner': 'Henry Hops',
    'description': 'Sample Widget',
    'otherAttributes': [{'name': 'size', 'value': '100'}]
}


# ----------------------------
# S3 Tests
# ----------------------------
@mock_aws()
def test_s3_store_widget():
    # Create mocked S3 client
    s3 = boto3.client("s3", region_name="us-east-1")
    bucket_name = "test-bucket"
    s3.create_bucket(Bucket=bucket_name)

    s3_manager = S3Manager(s3, bucket_name, use_owner_prefix=True)
    s3_manager.store_widget(SAMPLE_WIDGET)

    key = f"{SAMPLE_WIDGET['owner'].replace(' ', '_')}/{SAMPLE_WIDGET['widgetId']}.json"
    response = s3.get_object(Bucket=bucket_name, Key=key)
    content = json.loads(response['Body'].read())
    assert content['widgetId'] == SAMPLE_WIDGET['widgetId']


# ----------------------------
# DynamoDB Tests
# ----------------------------
@mock_aws()
def test_dynamodb_create_widget():
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    table_name = "Widgets"
    table = dynamodb.create_table(
        TableName=table_name,
        KeySchema=[{'AttributeName': 'id', 'KeyType': 'HASH'}],
        AttributeDefinitions=[{'AttributeName': 'id', 'AttributeType': 'S'}],
        BillingMode='PAY_PER_REQUEST'
    )

    db_manager = DynamoDBManager(dynamodb, table_name, key_name="id")
    db_manager.create_or_update_widget(SAMPLE_WIDGET)

    response = table.get_item(Key={'id': SAMPLE_WIDGET['widgetId']})
    item = response.get('Item', {})
    assert item['widgetId'] == SAMPLE_WIDGET['widgetId']
    assert item['owner'] == 'Henry Hops'


# ----------------------------
# SQS Tests
# ----------------------------
@mock_aws()
def test_sqs_receive_and_delete_message():
    sqs = boto3.client("sqs", region_name="us-east-1")
    queue = sqs.create_queue(QueueName="test-queue")
    queue_url = queue['QueueUrl']

    sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(SAMPLE_WIDGET))

    consumer = SQSConsumer(sqs, queue_url)
    messages = consumer.receive_messages()
    assert len(messages) == 1

    body = json.loads(messages[0]['Body'])
    assert body['widgetId'] == SAMPLE_WIDGET['widgetId']

    consumer.delete_message(messages[0]['ReceiptHandle'])
    messages_after = consumer.receive_messages()
    assert len(messages_after) == 0


# ----------------------------
# WidgetProcessor Tests
# ----------------------------
@mock_aws()
def test_widget_processor_create():
    # Set up S3 in mock
    s3_client = boto3.client("s3", region_name="us-east-1")
    bucket_name = "test-bucket"
    s3_client.create_bucket(Bucket=bucket_name)
    s3_manager = S3Manager(s3_client, bucket_name, use_owner_prefix=True)

    # Set up DynamoDB in mock
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    table_name = "Widgets"
    table = dynamodb.create_table(
        TableName=table_name,
        KeySchema=[{'AttributeName': 'id', 'KeyType': 'HASH'}],
        AttributeDefinitions=[{'AttributeName': 'id', 'AttributeType': 'S'}],
        BillingMode='PAY_PER_REQUEST'
    )
    db_manager = DynamoDBManager(dynamodb, table_name, key_name="id")

    processor = WidgetProcessor(s3_manager, db_manager)
    processor.process(SAMPLE_WIDGET)

    # Verify S3 object
    key = f"{SAMPLE_WIDGET['owner'].replace(' ', '_')}/{SAMPLE_WIDGET['widgetId']}.json"
    s3_obj = s3_client.get_object(Bucket=bucket_name, Key=key)
    loaded = json.loads(s3_obj['Body'].read())
    assert loaded['widgetId'] == SAMPLE_WIDGET['widgetId']

    # Verify DynamoDB item
    item = table.get_item(Key={'id': SAMPLE_WIDGET['widgetId']})['Item']
    assert item['widgetId'] == SAMPLE_WIDGET['widgetId']
