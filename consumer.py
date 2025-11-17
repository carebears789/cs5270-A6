import json
import time
import argparse
import boto3
import logging
from botocore.exceptions import ClientError
import sys

# ------------------------------------------------------
# SETUP LOGGING (FILE ONLY, CLEAN)
# ------------------------------------------------------

LOG_FILE = "widget_app.log"

# Remove existing handlers so nothing logs to terminal
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,                   # <--- CLEAN INFO LOGGING
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

logger = logging.getLogger("WidgetApp")


# Add a StreamHandler to also print to terminal
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S")
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

# ------------------------------------------------------
# AWS CLIENT FACTORY
# ------------------------------------------------------

class AWSClientFactory:
    def __init__(self, profile: str, region: str):
        logger.info(f"Initializing AWS session (profile={profile}, region={region})")
        self.session = boto3.Session(profile_name=profile, region_name=region)

    def sqs(self):
        return self.session.client("sqs")

    def s3(self):
        return self.session.client("s3")

    def dynamodb(self):
        return self.session.resource("dynamodb")


# ------------------------------------------------------
# S3 MANAGER
# ------------------------------------------------------

class S3Manager:
    def __init__(self, client, bucket: str, use_owner_prefix: bool):
        self.client = client
        self.bucket = bucket
        self.use_owner_prefix = use_owner_prefix

        if bucket:
            logger.info(f"S3 enabled → bucket={bucket}, prefix_by_owner={use_owner_prefix}")
        else:
            logger.info("S3 disabled (no bucket provided)")

    def store_widget(self, widget: dict):
        if not self.bucket:
            return

        owner = widget.get("owner", "unknown").replace(" ", "_")
        widget_id = widget["widgetId"]
        key = f"{owner}/{widget_id}.json" if self.use_owner_prefix else f"{widget_id}.json"

        logger.info(f"S3 WRITE → s3://{self.bucket}/{key}")

        try:
            self.client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=json.dumps(widget).encode("utf-8")
            )
        except Exception as e:
            logger.error(f"S3 write failed for {key}: {e}")
            raise


# ------------------------------------------------------
# DYNAMODB MANAGER
# ------------------------------------------------------

class DynamoDBManager:
    def __init__(self, dynamodb_resource, table_name="Widgets", key_name="id"):
        self.table = dynamodb_resource.Table(table_name)
        self.key_name = key_name
        logger.info(f"DynamoDB connected → table={table_name}, key={key_name}")

    def _map_widget_to_item(self, widget: dict):
        item = widget.copy()
        item[self.key_name] = widget["widgetId"]
        return item

    def create_or_update_widget(self, widget: dict):
        item = self._map_widget_to_item(widget)
        logger.info(f"DynamoDB UPSERT → {self.table.name}:{item[self.key_name]}")

        try:
            self.table.put_item(Item=item)
        except Exception as e:
            logger.error(f"DynamoDB upsert failed ({item[self.key_name]}): {e}")
            raise

    def delete_widget(self, widget_id: str):
        logger.info(f"DynamoDB DELETE → {self.table.name}:{widget_id}")

        try:
            self.table.delete_item(Key={self.key_name: widget_id})
        except Exception as e:
            logger.error(f"DynamoDB delete failed ({widget_id}): {e}")
            raise


# ------------------------------------------------------
# SQS CONSUMER
# ------------------------------------------------------

class SQSConsumer:
    def __init__(self, client, queue_url: str):
        self.client = client
        self.queue_url = queue_url
        logger.info(f"Connected to SQS queue → {queue_url}")

    def receive_messages(self):
        try:
            response = self.client.receive_message(
                QueueUrl=self.queue_url,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=10
            )
            messages = response.get("Messages", [])

            if messages:
                logger.info(f"SQS received {len(messages)} message(s)")

            return messages

        except Exception as e:
            logger.error(f"SQS receive failed: {e}")
            return []

    def delete_message(self, receipt_handle: str):
        try:
            self.client.delete_message(
                QueueUrl=self.queue_url,
                ReceiptHandle=receipt_handle
            )
            logger.info("SQS DELETE → message removed")
        except Exception as e:
            logger.error(f"SQS delete failed: {e}")
            raise


# ------------------------------------------------------
# WIDGET PROCESSOR
# ------------------------------------------------------

class WidgetProcessor:
    def __init__(self, s3_manager: S3Manager, dynamo_manager: DynamoDBManager):
        self.s3 = s3_manager
        self.db = dynamo_manager

    def process(self, widget: dict):
        wtype = widget["type"]
        widget_id = widget["widgetId"]

        logger.info(f"PROCESS → widgetId={widget_id}, type={wtype}")

        if wtype in ("create", "update"):
            self.s3.store_widget(widget)
            self.db.create_or_update_widget(widget)

        elif wtype == "delete":
            self.db.delete_widget(widget_id)

        else:
            logger.warning(f"Unknown widget type encountered: {wtype}")


# ------------------------------------------------------
# MAIN APP LOOP
# ------------------------------------------------------

class WidgetApp:
    def __init__(self, args):
        logger.info("WidgetApp starting…")
        logger.info(f"Profile={args.profile}, Region={args.region}, Queue={args.request_queue}")
        logger.info(f"Bucket={args.request_bucket}, Table={args.table_name}")

        aws = AWSClientFactory(args.profile, args.region)

        self.sqs = SQSConsumer(aws.sqs(), args.request_queue)
        self.s3 = S3Manager(aws.s3(), args.request_bucket, args.use_owner_in_prefix)
        self.db = DynamoDBManager(aws.dynamodb(), args.table_name, args.db_key_name)

        self.processor = WidgetProcessor(self.s3, self.db)

        self.max_runtime = args.max_runtime
        self.start_time = time.time()

    def run(self):
        logger.info("WidgetApp is running…")

        while True:
            # Check timeout
            if self.max_runtime > 0:
                elapsed = (time.time() - self.start_time) * 1000
                if elapsed > self.max_runtime:
                    logger.info("Max runtime reached → stopping")
                    break

            messages = self.sqs.receive_messages()
            if not messages:
                continue

            for msg in messages:
                try:
                    body = json.loads(msg["Body"])
                    self.processor.process(body)
                    self.sqs.delete_message(msg["ReceiptHandle"])
                except Exception as e:
                    logger.error(f"Process failed: {e}")
                    logger.error(f"Message body: {msg['Body']}")


# ------------------------------------------------------
# ARGUMENT PARSER
# ------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(description="Widget Processor")

    parser.add_argument("-p", "--profile", default="default")
    parser.add_argument("-r", "--region", default="us-east-1")
    parser.add_argument("-mrt", "--max-runtime", type=int, default=0)

    parser.add_argument("-rb", "--request-bucket", default=None)
    parser.add_argument("-uop", "--use-owner-in-prefix", action="store_true")

    parser.add_argument("-rq", "--request-queue", required=True)

    parser.add_argument("--table-name", default="Widgets")
    parser.add_argument("--db-key-name", default="id")

    return parser.parse_args()


# ------------------------------------------------------
# ENTRY POINT
# ------------------------------------------------------

if __name__ == "__main__":
    args = parse_args()
    app = WidgetApp(args)
    app.run()
