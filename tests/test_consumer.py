import json
import time
import pytest
import boto3
from moto import mock_s3, mock_dynamodb
from consumer import (
    get_next_request,
    process_widget_request,
    store_widget_s3,
    store_widget_dynamo
)

def test_get_next_request():
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="usu-cs5270-scuba-requests")

    request= [
        {"action": "create", "widget": {"id": "a", "name": "test1"}},
        {"action": "create", "widget": {"id": "b", "name": "test2"}},
    ]
    s3.put_object(Bucket="bucket2", Key="001.json", Body=json.dumps(requests[0]))
    s3.put_object(Bucket="bucket2", Key="002.json", Body=json.dumps(requests[1]))

    result = get_next_request(s3, "usu-cs5270-scuba-requests")

    assert result["widget"]["id"] == "a"
    keys = [obj["Key"] for obj in s3.list_objects_v2(Bucket="usu-cs5270-scuba-requests")["Contents"]]
    assert "001.json" not in keys

def test_store_widget_s3_in_bucket():
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="usu-cs5270-scuba-web")

    widget = {"id": "567", "name": "test1"}

    store_widget_s3(s3, "usu-cs5270-scuba-web", widget)

    response = s3.get_object(Bucket="usu-cs5270-scuba-web", Key="567.json")
    data = json.loads(obj["Body"].read())
    assert data["name"] == "test1"

