import argparse
import boto3
import json
import time
import logging

def get_request(s3, bucket_name):
    response = s3.list_objects_v2(Bucket=bucket_name)
    if "Contents" not in response:
        return None
    
    keys = sorted(obj["Key"] for obj in response["Contents"])
    next_key = keys[0]
    obj = s3.get_object(Bucket=bucket_name, Key=next_key)
    data = json.loads(obj["Body"].read())

    s3.delete_object(Bucket=bucket_name, Key=next_key)
    return data

def store_in_s3(s3, bucket_name, widget):
    processed_owner = widget['owner'].replace(' ', '-').lower()

    widget_id = widget['widgetId']

    key = f"widgets/{processed_owner}/{widget_id}"
    print(key)
    s3.put_object(Bucket=bucket_name, Key=key, Body=json.dumps(widget))

def dynamo(dynamo, widget):
    dynamo_item = widget.copy()

    other_attributes_list = dynamo_item.pop('otherAttributes', [])

    for attr in other_attributes_list:
        if 'name' in attr and 'value' in attr:
            dynamo_item[attr['name']] = attr['value']

    dynamo.put_item(Item=dynamo_item)
    
def process_request(request, storage, s3, dyanmo, target):

    #print(f"Processing request: {request}")
    action = request.get("type")
    widget = request.get("widget")

    if action == "create" or action == "update":
        if storage == "s3":
            print(action)
            store_in_s3(s3, target, request)
        else:
            dynamo(dynamo, widget)
    elif action == "delete":
        logging.info("Delet not implemented yet")
    else:
        logging.info(f"Unknown action: {action}")

def main():
    parser = argparse.ArgumentParser(description="Widget Consumer")
    parser.add_argument("--storage", choices=["s3", "dynamo"], required=True,
                        help="Storage backend for widgets")
    parser.add_argument("--interval", type=int, default=.1, help="Polling interval in seconds")
    parser.add_argument("--table", help="DynamoDB table name (if using DynamoDB storage)")
    parser.add_argument("--bucket", help="Bucket table name (if using S3 storage)")
    args = parser.parse_args()
    


    s3 = boto3.client("s3")
    dynamo = boto3.resource("dynamodb").Table(args.table) if args.table else None
    while True:
        request = get_request(s3, "usu-cs5270-scuba-requests")
        if request:
            process_request(request, args.storage, s3, dynamo, "usu-cs5270-scuba-web")
        else:
            time.sleep(args.interval)

if __name__ == "__main__":
    main()