import json
import time
import argparse
import boto3
import logging
from botocore.exceptions import ClientError
import sys

# ------------------------------------------------------
# SETUP LOGGING
# ------------------------------------------------------
LOG_FILE = "widget_app.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout) # Log to console
    ]
)
logger = logging.getLogger("WidgetApp")

# ------------------------------------------------------
# AWS CLIENT FACTORY
# ------------------------------------------------------
class AWSClientFactory:
    """Creates AWS clients from a single session."""
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
# WIDGET STORAGE (SIMPLIFIED)
# ------------------------------------------------------
class WidgetStorage:
    """Handles simple storage to S3 and DynamoDB."""
    def __init__(self, s3_client, db_resource, db_table_name, s3_bucket_name, s3_key_prefix):
        self.s3 = s3_client
        self.db = db_resource
        self.s3_bucket = s3_bucket_name
        self.s3_prefix = s3_key_prefix
        
        if db_table_name:
            self.db_table = db_resource.Table(db_table_name)
            logger.info(f"DynamoDB storage enabled → {db_table_name}")
        else:
            self.db_table = None
            logger.info("DynamoDB storage disabled.")

        if s3_bucket_name:
            logger.info(f"S3 storage enabled → s3://{s3_bucket_name}/{s3_key_prefix}")
        else:
            logger.info("S3 storage disabled.")

    def _get_s3_key(self, widget: dict) -> str:
        """
        Generates the S3 key.
        Format: widgets/{owner-with-dashes}/{widget-id}.json
        """
        widget_id = widget.get("widgetId", "unknown-id")
        owner = widget.get("owner", "unknown-owner")
        
        # Format: replace spaces with dashes, lowercase
        formatted_owner = owner.replace(" ", "-").lower()
        
        # Format: widgets/{owner}/{widget id}
        return f"{self.s3_prefix}{formatted_owner}/{widget_id}.json"

    def create_or_update(self, widget: dict):
        """Simple 'upsert' for both create and update."""
        widget_id = widget["widgetId"]
        
        # Store in DynamoDB
        if self.db_table:
            logger.info(f"DynamoDB UPSERT → {self.db_table.name}:{widget_id}")
            
            # --- FIX ---
            # Create a new item dict and map 'widgetId' to 'id'
            item_to_save = widget.copy()
            item_to_save['id'] = widget['widgetId'] # Map to the 'id' key
            # --- END FIX ---
            
            # Save the new item, which now has the 'id' key
            self.db_table.put_item(Item=item_to_save)

        # Store in S3
        if self.s3_bucket:
            key = self._get_s3_key(widget)
            logger.info(f"S3 WRITE → s3://{self.s3_bucket}/{key}")
            self.s3.put_object(
                Bucket=self.s3_bucket,
                Key=key,
                Body=json.dumps(widget).encode("utf-8")
            )

    def delete(self, widget: dict):
        """Simple delete for 'delete' request."""
        widget_id = widget["widgetId"]
        
        # Delete from DynamoDB
        if self.db_table:
            logger.info(f"DynamoDB DELETE → {self.db_table.name}:{widget_id}")
            
            # --- FIX ---
            # Use the table's primary key 'id'
            self.db_table.delete_item(
                Key={"id": widget_id}, 
                ConditionExpression="attribute_exists(id)"
            )
            # --- END FIX ---

        # Delete from S3
        if self.s3_bucket:
            key = self._get_s3_key(widget)
            logger.info(f"S3 DELETE → s3://{self.s3_bucket}/{key}")
            self.s3.delete_object(Bucket=self.s3_bucket, Key=key)

# ------------------------------------------------------
# REQUEST POLLERS
# ------------------------------------------------------
class SqsPoller:
    """Polls SQS, processes messages, and deletes them."""
    def __init__(self, sqs_client, queue_url: str):
        self.sqs = sqs_client
        self.queue_url = queue_url
        logger.info(f"Using SQS request source → {queue_url}")

    def poll(self):
        """Polls SQS for messages."""
        response = self.sqs.receive_message(
            QueueUrl=self.queue_url,
            MaxNumberOfMessages=10,  # Read up to 10 at a time
            WaitTimeSeconds=20       # Use long polling
        )
        messages = response.get("Messages", [])
        
        if messages:
            logger.info(f"SQS received {len(messages)} message(s)")
        
        return messages

    def delete_message(self, message: dict):
        """Deletes a message from the SQS queue."""
        receipt_handle = message["ReceiptHandle"]
        self.sqs.delete_message(
            QueueUrl=self.queue_url,
            ReceiptHandle=receipt_handle
        )
        logger.info("SQS DELETE → message removed")

class S3Poller:
    """Polls S3 for request files."""
    def __init__(self, s3_client, bucket_name: str):
        self.s3 = s3_client
        self.bucket = bucket_name
        logger.info(f"Using S3 request source → {bucket_name}")

    def poll(self):
        """Polls for one message from S3."""
        response = self.s3.list_objects_v2(Bucket=self.bucket, MaxKeys=1)
        if "Contents" not in response or not response["Contents"]:
            return [] # No messages
        
        s3_key = response["Contents"][0]["Key"]
        logger.info(f"S3 received message → {s3_key}")

        # Read the object
        obj = self.s3.get_object(Bucket=self.bucket, Key=s3_key)
        body_str = obj['Body'].read().decode('utf-8')
        
        # Return in a compatible format
        message = {
            "Body": body_str,
            "S3Key": s3_key, # For deletion
            "ReceiptHandle": s3_key # For compatibility
        }
        return [message]

    def delete_message(self, message: dict):
        """Deletes a message from the S3 bucket."""
        s3_key = message["S3Key"]
        self.s3.delete_object(Bucket=self.bucket, Key=s3_key)
        logger.info(f"S3 DELETE → request removed: {s3_key}")

# ------------------------------------------------------
# MAIN APP
# ------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Widget Consumer")
    
    # AWS Config
    parser.add_argument("-p", "--profile", default="default", help="AWS profile name")
    parser.add_argument("-r", "--region", default="us-east-1", help="AWS region")
    
    # --- Request Source ---
    # Use a mutually exclusive group to enforce one or the other
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("-rq", "--request-queue", default=None, help="SQS request queue URL")
    group.add_argument("-srb", "--s3-request-bucket", default=None, help="S3 request bucket name")

    # --- Storage Destination ---
    parser.add_argument("-dwt", "--dynamodb-widget-table", default="widgets", help="DynamoDB table for widgets")
    parser.add_argument("-swb", "--s3-widget-bucket", default=None, help="S3 bucket for widgets (optional)")
    parser.add_argument("-swkp", "--s3-widget-key-prefix", default="widgets/", help="S3 key prefix for widgets")

    args = parser.parse_args()

    logger.info("Starting Widget Consumer App...")
    
    # Setup dependencies
    aws = AWSClientFactory(args.profile, args.region)
    storage = WidgetStorage(
        s3_client=aws.s3(),
        db_resource=aws.dynamodb(),
        db_table_name=args.dynamodb_widget_table,
        s3_bucket_name=args.s3_widget_bucket,
        s3_key_prefix=args.s3_widget_key_prefix
    )
    
    # --- CHOOSE POLLER ---
    if args.request_queue:
        poller = SqsPoller(aws.sqs(), args.request_queue)
    elif args.s3_request_bucket:
        poller = S3Poller(aws.s3(), args.s3_request_bucket)
    else:
        # This part is technically unreachable due to the "required=True" group
        logger.critical("No request source specified. Exiting.")
        sys.exit(1)

    # Run loop
    logger.info("Application running. Polling for messages...")
    while True:
        messages = poller.poll()
        if not messages:
            # If S3, we need to wait manually
            if isinstance(poller, S3Poller):
                time.sleep(5) # S3 polling can be slower
            continue # SQS long polling already waited
        
        for msg in messages:
            widget = json.loads(msg["Body"])
            request_type = widget.get("type", "unknown")
            widget_id = widget.get("widgetId", "unknown")
            
            logger.info(f"PROCESS → widgetId={widget_id}, type={request_type}")

            if request_type in ("create", "update"):
                storage.create_or_update(widget)
            elif request_type == "delete":
                storage.delete(widget) # Pass full widget for S3 key
            else:
                logger.warning(f"Unknown request type '{request_type}' for {widget_id}")
            
            # If processing was successful, delete message
            poller.delete_message(msg)

if __name__ == "__main__":
    main()