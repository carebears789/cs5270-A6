# cs5270-A6

# TO Run I used Poetry and main files is consumer and log in consumer.log
# Test is in test_consumer.py

# Widget Consumer

This program polls an S3 bucket for incoming widget creation/update requests and stores the processed widget data into a target storage backend, which can be **AWS S3** or **AWS DynamoDB**.

## Prerequisites

1.  **Python 3.x:** Ensure you have a compatible Python version installed.
2.  **AWS CLI Configured:** The program uses `boto3`, which relies on your local AWS credentials being set up (e.g., via `~/.aws/credentials` or IAM roles).
3.  **Required AWS Resources:**
    * **Source S3 Bucket:** A bucket where incoming request files are placed.
    * **Target Storage:** Either an S3 bucket or a DynamoDB table, depending on the `--storage` argument.

## Installation

1.  **Install Dependencies:**
    ```bash
    pip install boto3
    ```

## Program Execution

The program is run from the command line and requires the `--storage` and `--source-bucket` arguments. The required arguments for the target storage are conditional.

### General Command Syntax

```bash
python widget_consumer.py --storage <s3|dynamo> --source-bucket <request_bucket> [TARGET_ARGS] [OPTIONS]