import argparse
import boto3
import json
import time
import logging
import sys

# Define the maximum time to wait for a new request before exiting (30 seconds)
MAX_WAIT_TIME = 30


def validate_args(args):
    """
    Validates input arguments based on the selected storage backend.

    Exits the program if required arguments are missing.
    """
    if args.storage == "dynamo" and not args.table:
        print("🛑 Error: If 'dynamo' is selected for storage, --table is required.")
        sys.exit(1)
    
    if args.storage == "s3" and not args.target_bucket:
        print("🛑 Error: If 's3' is selected for storage, --target-bucket is required.")
        sys.exit(1)

    if not args.source_bucket:
        print("🛑 Error: --source-bucket is required for polling requests.")
        sys.exit(1)

# Renaming args.bucket to args.source_bucket for clarity in main/argparse
def get_request(s3, source_bucket_name):
    """
    Polls the source S3 bucket for the next widget request file.
    
    The request file is the one with the lexicographically smallest key.
    It returns the request data and deletes the file upon success.

    :param s3: Boto3 S3 client object.
    :param source_bucket_name: Name of the S3 bucket to poll requests from.
    :return: The request data (dict) or None if no files are found.
    """
    try:
        response = s3.list_objects_v2(Bucket=source_bucket_name, MaxKeys=1)
        
        if "Contents" not in response:
            return None
        
        # Get the key of the first (smallest) object
        next_key = response["Contents"][0]["Key"]
        
        # Retrieve object content
        obj = s3.get_object(Bucket=source_bucket_name, Key=next_key)
        data = json.loads(obj["Body"].read())

        # Delete the processed object
        s3.delete_object(Bucket=source_bucket_name, Key=next_key)
        
        return data
    except Exception as e:
        logging.error(f"Error getting or deleting request from S3: {e}")
        return None

def store_in_s3(s3, target_bucket_name, request):
    """
    Stores the processed widget data into the target S3 bucket.

    The key format is: widgets/<owner_name_slug>/<widget_id>

    :param s3: Boto3 S3 client object.
    :param target_bucket_name: Name of the S3 bucket to store processed widgets in.
    :param request: The incoming request dictionary containing the 'widget'.
    """
    widget = request.get('widget', {})
    if not widget:
        logging.warning("Cannot store in S3: 'widget' not found in request.")
        return
        
    processed_owner = widget.get('owner', 'unknown').replace(' ', '-').lower()
    widget_id = widget.get('widgetId', str(time.time())) # Fallback widgetId

    key = f"widgets/{processed_owner}/{widget_id}.json"
    
    # Store the entire request, or just the widget, depending on requirements.
    # Sticking to the original code's logic: store the request content.
    logging.info(f"Storing widget to S3 at key: {key}")
    try:
        s3.put_object(
            Bucket=target_bucket_name, 
            Key=key, 
            Body=json.dumps(request)
        )
    except Exception as e:
        logging.error(f"Error storing to S3: {e}")


def dynamo_store(dynamo_table, widget):
    """
    Stores the widget data into the DynamoDB table.

    It flattens attributes from the 'otherAttributes' list into top-level keys.

    :param dynamo_table: Boto3 DynamoDB Table resource object.
    :param widget: The widget dictionary to be stored.
    """
    dynamo_item = widget.copy()
    
    other_attributes_list = dynamo_item.pop('otherAttributes', [])

    for attr in other_attributes_list:
        # Check for required keys and ensure they are not None before assignment
        if 'name' in attr and 'value' in attr and attr['name'] and attr['value'] is not None:
            dynamo_item[attr['name']] = attr['value']
    
    logging.info(f"Storing widget ID {widget.get('widgetId')} to DynamoDB.")
    try:
        dynamo_table.put_item(Item=dynamo_item)
    except Exception as e:
        logging.error(f"Error storing to DynamoDB: {e}")


def process_request(request, storage, s3, dynamo_table, target_bucket):
    """
    Processes a single widget request based on the action type and storage backend.

    :param request: The request dictionary received from the source S3 bucket.
    :param storage: The selected storage backend ("s3" or "dynamo").
    :param s3: Boto3 S3 client object.
    :param dynamo_table: Boto3 DynamoDB Table resource object.
    :param target_bucket: Name of the S3 bucket for storing processed widgets.
    """
    action = request.get("type")
    widget = request.get("widget")

    if not action or not widget:
        logging.warning(f"Malformed request received: {request}")
        return

    if action == "create" or action == "update":
        if storage == "s3":
            logging.info(f"Processing '{action}' action for S3 storage.")
            store_in_s3(s3, target_bucket, request)
        else:
            logging.info(f"Processing '{action}' action for DynamoDB storage.")
            dynamo_store(dynamo_table, widget)
            
    elif action == "delete":
        logging.info(f"Action 'delete' not implemented yet for widget ID: {widget.get('widgetId')}.")
    else:
        logging.warning(f"Unknown action type received: {action}")

def main():
    """
    Main execution function. Sets up logging, parses arguments, and starts the polling loop.
    """
    
    # --- Logging Setup ---
    logging.basicConfig(
        filename='consumer.log',
        level=logging.INFO,
        format='%(asctime)s %(levelname)s:%(message)s'
    )
    logging.info("Consumer process starting...")
    
    # --- Argument Parsing ---
    parser = argparse.ArgumentParser(description="Widget Consumer: Polls S3 for widget requests and stores them in S3 or DynamoDB.")
    parser.add_argument("--storage", choices=["s3", "dynamo"], required=True,
                        help="Storage backend for processed widgets.")
    parser.add_argument("--interval", type=float, default=0.1, 
                        help="Polling interval in seconds (default: 0.1).")
    parser.add_argument("--source-bucket", required=True, 
                        help="S3 bucket name where incoming requests are placed.")
    parser.add_argument("--table", 
                        help="DynamoDB table name (if using DynamoDB storage).")
    parser.add_argument("--target-bucket", 
                        help="S3 bucket name where processed widgets are stored (if using S3 storage).")
    args = parser.parse_args()

    # --- Input Validation ---
    validate_args(args)
    
    # --- AWS Client Setup ---
    try:
        s3 = boto3.client("s3")
        dynamo_table = boto3.resource("dynamodb").Table(args.table) if args.table else None
    except Exception as e:
        logging.error(f"Failed to initialize AWS clients: {e}")
        sys.exit(1)


    # --- Polling Loop ---
    start_time_no_request = None # Time marker for when the loop first found no requests

    while True:
        request = get_request(s3, args.source_bucket)
        
        if request:
            # Request found: process it and reset the wait timer
            logging.info("Request received. Processing...")
            process_request(request, args.storage, s3, dynamo_table, args.target_bucket)
            start_time_no_request = None
            
        else:
            # No request found: check the timeout condition
            if start_time_no_request is None:
                start_time_no_request = time.time()
                logging.info(f"No requests found. Starting max wait timer of {MAX_WAIT_TIME} seconds.")
            
            elapsed_time_no_request = time.time() - start_time_no_request
            
            if elapsed_time_no_request >= MAX_WAIT_TIME:
                logging.info(f"Max wait time of {MAX_WAIT_TIME} seconds reached without new requests. Exiting consumer.")
                break # Exit the infinite loop
            
            # Wait for the next poll interval
            time.sleep(args.interval)

    logging.info("Consumer process shut down gracefully.")


if __name__ == "__main__":
    main()