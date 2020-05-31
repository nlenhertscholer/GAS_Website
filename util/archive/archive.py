# archive.py
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
from botocore.exceptions import ClientError
import json
from configparser import ConfigParser
import psycopg2
import io

# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers

# Get configuration
config = ConfigParser(os.environ)
config.read('archive_config.ini')

# Connect to SQS and get the message queue
queue_url = config['aws']["SQSArchiveURL"]
sqs = boto3.resource("sqs", region_name=config['aws']['RegionName'])
queue = sqs.Queue(queue_url)

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

        # Extract parameters from message
        user_id = input_data['user_id']
        job_id = input_data['job_id']
        results_file = input_data['results_file']

        # Retrieve user information and begin formatting email
        success = True
        try:
            profile = helpers.get_user_profile(user_id)
        except (ClientError, psycopg2.Error) as e:
            print(f"Unable to retrieve user details: {e.response['Error']['Message']}")
            success = False

        if success:
            # Upload free_user files to glacier
            if profile['role'] == "free_user":

                # Retrieve the S3 file
                log_file = None
                try:
                    # Read the results file directly from S3 to send to Glacier
                    # https://stackoverflow.com/a/48696641
                    s3 = boto3.resource('s3', region_name=config['aws']["RegionName"])
                    s3_results_bucket = s3.Bucket(config['aws']['S3ResultsBucket'])
                    log_file = s3_results_bucket.Object(results_file)

                except ClientError as e:
                    print(f"Unable to retrieve results file from S3: {e.response['Error']['Message']}")
                    success = False

                # Upload the file to glacier
                # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier.html#vault
                if success:
                    try:
                        glacier = boto3.resource('glacier', region_name=config['aws']['RegionName'])
                        vault = glacier.Vault(account_id='-', name=config['aws']['GlacierName'])
                        archive = vault.upload_archive(body=io.BytesIO(log_file.get()['Body'].read()))
                        print(f"Successfully Uploaded File with job_id {job_id} to Glacier")

                        # Delete the log file
                        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Object.delete
                        log_file.delete()
                    except ClientError as e:
                        print(f"Error in uploading file to Glacier: {e.response['Error']['Message']}")
                        success = False

                # Persist archive id to database
                if success:
                    try:
                        db = boto3.resource('dynamodb', region_name=config['aws']['RegionName'])
                        table = db.Table(config['aws']['DynamoTable'])
                        table.update_item(
                            Key={
                                'job_id': job_id
                            },
                            UpdateExpression='SET results_file_archive_id = :val1',
                            ExpressionAttributeValues={
                                # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier.html#Glacier.Archive.id
                                ':val1': archive.id
                            }
                        )
                    except ClientError as e:
                        print(f"Unable to update database with Glacier Archive ID: "
                              f"{e.response['Error']['Message']}")

        # If everything happened successfully then delete the message
        if success:
            message[0].delete()







### EOF