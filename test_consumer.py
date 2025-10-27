import unittest
import json
import boto3
from moto import mock_aws
from unittest.mock import MagicMock, patch
import consumer as widget_consumer
from consumer import get_request, store_in_s3, dynamo_store

class TestConsumer(unittest.TestCase):
    def test_get_next_request(self):
        with mock_aws():
            s3 = boto3.client("s3", region_name="us-east-1")
            s3.create_bucket(Bucket="usu-cs5270-scuba-requests")

            request= [
                {"action": "create", "widget": {"id": "a", "name": "test1"}},
                {"action": "create", "widget": {"id": "b", "name": "test2"}},
            ]
            s3.put_object(Bucket="usu-cs5270-scuba-requests", Key="001.json", Body=json.dumps(request[0]))
            s3.put_object(Bucket="usu-cs5270-scuba-requests", Key="002.json", Body=json.dumps(request[1]))

            result = get_request(s3, "usu-cs5270-scuba-requests")

            self.assertEqual(result["widget"]["id"], "a")
            keys = [obj["Key"] for obj in s3.list_objects_v2(Bucket="usu-cs5270-scuba-requests")["Contents"]]
            self.assertNotIn("001.json", keys)

    def test_store_widget_dynamo(self):
        with mock_aws():
            dyanmo_test = boto3.resource("dynamodb", region_name="us-east-1")

            table = dyanmo_test.create_table(
                TableName="widgets",
                KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
                AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
                BillingMode="PAY_PER_REQUEST"
            )

            widget = {"id": "001", "name": "TestWidget"}
            table.put_item(Item=widget)

            result = table.get_item(Key={"id": "001"})
            self.assertEqual(result["Item"]["name"], "TestWidget")
    def test_get_request_returns_none_if_no_contents(self):
        s3 = MagicMock()
        s3.list_objects_v2.return_value = {}
        result = widget_consumer.get_request(s3, "test-bucket")
        self.assertIsNone(result)
        s3.get_object.assert_not_called()
        s3.delete_object.assert_not_called()

    def test_get_request_returns_data_and_deletes_object(self):
        s3 = MagicMock()
        s3.list_objects_v2.return_value = {
            "Contents": [{"Key": "file1.json"}, {"Key": "file2.json"}]
        }
        fake_data = {"widgetId": "123"}
        s3.get_object.return_value = {
            "Body": MagicMock(read=MagicMock(return_value=json.dumps(fake_data).encode()))
        }

        result = widget_consumer.get_request(s3, "test-bucket")

        self.assertEqual(result, fake_data)
        s3.get_object.assert_called_once_with(Bucket="test-bucket", Key="file1.json")
        s3.delete_object.assert_called_once_with(Bucket="test-bucket", Key="file1.json")

    def test_store_in_s3_puts_object(self):
        s3 = MagicMock()
        widget = {
            "owner": "John Doe",
            "widgetId": "abc"
        }

        widget_consumer.store_in_s3(s3, "test-bucket", widget)

        expected_key = "widgets/john-doe/abc"
        s3.put_object.assert_called_once()
        args, kwargs = s3.put_object.call_args
        self.assertEqual(kwargs["Key"], expected_key)
        body_data = json.loads(kwargs["Body"])
        self.assertEqual(body_data, widget)


if __name__ == "__main__":
    unittest.main()
