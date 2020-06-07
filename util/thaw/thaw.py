# thaw.py
#
# NOTE: This file lives on the Utils instance
#
# Copyright (C) 2011-2019 Vas Vasiliadis
# University of Chicago
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import os
import sys
import boto3
import json
from botocore.exceptions import ClientError
from configparser import ConfigParser

# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers

# Get configuration
config = ConfigParser(os.environ)
config.read('thaw_config.ini')

# Connect to SQS and get the message queue
try:
    queue_url = config['aws']["SQSThawURL"]
    sqs = boto3.resource("sqs", region_name=config['aws']['RegionName'])
    queue = sqs.Queue(queue_url)
except ClientError as e:
    print(f"Unable to connect to Archive Thaw SQS: {e.response['Error']['Message']}")
    exit(1)


def extract_uid(s3_key):
    """Extract the user job id from the s3 key passed in"""

    uid_file = s3_key.split('/')[2]
    return uid_file.split('~')[0]


# Add utility code here
while True:

    # Attempt to read a message from the queue
    seconds_to_wait = int(config['aws']['WaitTime'])
    message = queue.receive_messages(
        WaitTimeSeconds=seconds_to_wait
    )

    # Process the retrieved message
    if message:
        body = json.loads(message[0].body)
        input_data = json.loads(body["Message"])

        # Extract the parameters
        s3_results_key = input_data['JobDescription']
        job_id = input_data['JobId']
        archive_id = input_data['ArchiveId']

        success = True
        try:
            # Retrieve file from glacier
            glacier = boto3.client('glacier', region_name=config['aws']['RegionName'])
            response = glacier.get_job_output(vaultName=config['aws']['GlacierName'],
                                              jobId=job_id)
        except ClientError as e:
            print(f"Unable to get job output from glacier: {e.response['Error']['Message']}")
            success = False

        if success:
            # Upload file to S3
            try:
                s3 = boto3.resource('s3', region_name=config['aws']['RegionName'])
                bucket = s3.Bucket(config['aws']['S3ResultsBucket'])
                bucket.upload_fileobj(response['body'], s3_results_key)
            except ClientError as e:
                print(f"Unable to upload file to S3: {e.response['Error']['Message']}")
                success = False

        if success:
            # Delete file from glacier
            try:
                glacier.delete_archive(vaultName=config['aws']['GlacierName'],
                                       archiveId=archive_id)
                # Delete the message here since we don't want to reprocess it
                message[0].delete()
            except ClientError as e:
                print(f"Unable to delete archive: {e.response['Error']['Message']}")
                success = False

        if success:
            # Update DynamoDB
            try:
                db = boto3.resource('dynamodb', region_name=config['aws']['RegionName'])
                table = db.Table(config['aws']['DynamoTable'])
                table.update_item(
                    Key={
                        'job_id': extract_uid(s3_results_key)
                    },
                    UpdateExpression='REMOVE #RESTKEY',
                    ExpressionAttributeNames={
                        "#RESTKEY": config["aws"]["RestoreKey"]
                    }
                )
            except ClientError as e:
                print(f"Unable to remove restore key: "
                      f"{e.response['Error']['Message']}")

### EOF