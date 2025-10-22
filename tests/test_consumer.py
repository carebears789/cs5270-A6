import unittest
import json
import boto3
from moto import mock_aws
from consumer import get_request, store_in_s3, dynamo

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
            dynamo = boto3.resource("dynamodb", region_name="us-east-1")

            # Create table
            table = dynamo.create_table(
                TableName="widgets",
                KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
                AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
                BillingMode="PAY_PER_REQUEST"
            )

            widget = {"id": "001", "name": "TestWidget"}
            table.put_item(Item=widget)

            result = table.get_item(Key={"id": "001"})
            self.assertEqual(result["Item"]["name"], "TestWidget")


if __name__ == "__main__":
    unittest.main()
