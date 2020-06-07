# Homework 5 Annotator Script
# by Nesta Lenhert-Scholer

import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Attr
import subprocess
import os
import json
from configparser import ConfigParser

# Initialize Config Parser
config = ConfigParser(os.environ)
config.read("ann_config.ini")

# File paths
RUN = config['file']['Run']
JOBS = config['file']['Jobs']


def run_annotation_job(file, uid, user):
    """Runs the annotation job on the downloaded file"""

    try:
        subprocess.Popen(['python', RUN, file, uid, user],
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        return True

    except (subprocess.SubprocessError, OSError) as e:
        print("Unable to run the annotation job", e)
        return False


def download_file(bucket, key, uid, file):
    """Downloads the annotation file from S3 and stores it in the local filesystem"""

    # Create the necessary folders
    # https://docs.python.org/3/library/os.html#os.makedirs
    folder_path = os.path.join(JOBS, uid)
    os.makedirs(folder_path, exist_ok=True)

    # Download the file from s3
    # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-example-download-file.html
    try:
        s3 = boto3.client('s3', region_name=config['aws']['RegionName'])
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.download_fileobj
        with open(os.path.join(folder_path, file), 'wb') as f:
            s3.download_fileobj(bucket, key, f)

        return os.path.join(folder_path, file), uid
    except ClientError:
        raise


def process_job(data):
    """Download the file and start the annotation job"""

    # Extract job parameters from the request body
    try:
        bucket = data["s3_inputs_bucket"]
        key = data["s3_key_input_file"]
        job_id = data["job_id"]
        upload_file = data["input_file_name"]
        user_id = data["user_id"]
    except KeyError as e:
        print("Bad Key to retrieve data:", e)
        return False

    # Download the file and store it locally
    try:
        file, uid = download_file(bucket, key, job_id, upload_file)
    except ClientError as e:
        print("Unable to download file from S3:", e)
        return False

    # Launch the annotation job
    response = run_annotation_job(file, uid, user_id)

    # Change the job_status key in the annotations table to "RUNNING"
    if response:         # Make sure it was successful in launching the process
        # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/dynamodb.html#updating-item
        table = config['aws']['DynamoTable']
        db = boto3.resource('dynamodb', region_name=config['aws']['RegionName'])
        table = db.Table(table)
        try:
            table.update_item(
                Key={
                    'job_id': job_id
                },
                UpdateExpression='SET job_status = :val1',
                # Exercise 4 reference
                # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Table.update_item
                ConditionExpression=Attr('job_status').eq('PENDING'),
                ExpressionAttributeValues={
                    ':val1': "RUNNING"
                }
            )
        except db.meta.client.exceptions.ConditionalCheckFailedException as e:
            # Conditional check failed
            print("Conditional Check Failed:", e)

    return response


# Run the annotator
if __name__ == "__main__":

    # Connect to SQS and get the message queue
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Queue
    try:
        queue_url = config['aws']['SQSUrl']
        sqs = boto3.resource("sqs", region_name=config['aws']['RegionName'])
        queue = sqs.Queue(queue_url)
    except ClientError as e:
        print(f"Unable to connect to Job Request SQS: {e.response['Error']['Message']}")
        exit(1)

    while True:

        # Attempt to read a message from the queue
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Queue.receive_messages
        seconds_to_wait = int(config['aws']['WaitTime'])
        message = queue.receive_messages(
            WaitTimeSeconds=seconds_to_wait
        )

        # Process the retrieved message
        if message:
            body = json.loads(message[0].body)
            input_data = json.loads(body["Message"])

            # Launch the annotation job
            successful = process_job(input_data)

            if successful:
                # Delete the message from the queue,
                # if job was successfully submitted
                message[0].delete()
