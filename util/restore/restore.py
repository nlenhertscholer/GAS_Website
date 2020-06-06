# restore.py
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
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
import json
from configparser import ConfigParser

# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers

# Get configuration
config = ConfigParser(os.environ)
config.read('restore_config.ini')

# Connect to SQS and get the message queue
queue_url = config['aws']["SQSRestoreURL"]
sqs = boto3.resource("sqs", region_name=config['aws']['RegionName'])
queue = sqs.Queue(queue_url)

archive_key = config["aws"]["ArchiveKey"]

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

        # Extract user_id from message
        user_id = input_data['user_id']
        print(f"Processing archives for {user_id}")     # Print for logging purposes

        # Get DynamoDB data
        # Query the database
        index = config['aws']['DynamoUserIdIndex']
        key = config['aws']["DyanmoUserKey"]
        try:
            db = boto3.resource('dynamodb', region_name=config["aws"]["RegionName"])
            table = db.Table(config['aws']["DynamoTable"])
            results = table.query(
                IndexName=index,
                KeyConditionExpression=Key(key).eq(user_id)
            )
        except ClientError as e:
            print(f"Unable to retrieve table data for {user_id}: {e.response['Error']['Message']}")
            continue

        data = results["Items"]

        total_success = True
        # Process each database object
        for item in data:
            job_success = True
            if archive_key in item:
                s3_results_key = item["s3_key_result_file"]
                job_id = item["job_id"]
                archive_id = item[archive_key]

                job_params = {
                    "Type": config['aws']['Type'],
                    "ArchiveId": archive_id,
                    "Description": s3_results_key,
                    "SNSTopic": config["aws"]["SNSThawTopic"],
                    "Tier": config["aws"]["Fast"]
                }

                # Trigger archive retrieval
                # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier.html#Glacier.Client.initiate_job
                glacier = boto3.client('glacier', region_name=config['aws']['RegionName'])
                try:
                    # Run the expedited retrieval
                    response = glacier.initiate_job(
                        vaultName=config['aws']["GlacierName"],
                        jobParameters=job_params
                    )
                except ClientError as e:
                    print("Slow retrieval")
                    try:
                        job_params["Tier"] = config["aws"]["Slow"]
                        response = glacier.initiate_job(
                            vaultName=config['aws']["GlacierName"],
                            jobParameters=job_params
                        )
                    except ClientError as e:
                        print(f"Error in restoring file from Glacier: {e.response['Error']['Message']}")
                        total_success = False
                        job_success = False

                # Update database for object
                if job_success:
                    try:
                        table.update_item(
                            Key={
                                'job_id': job_id
                            },
                            UpdateExpression='SET restore_message = :val1 REMOVE #ARCHKEY',
                            ExpressionAttributeValues={
                                # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier.html#Glacier.Archive.id
                                ':val1': config["aws"]["RestoreMessage"],
                            },
                            ExpressionAttributeNames={
                                # I know it's ruby but it gave me the idea to use this parameter
                                # https://stackoverflow.com/questions/45553443/how-to-separate-multiple-clauses-in-a-dynamodb-update-expression
                                "#ARCHKEY": config["aws"]["ArchiveKey"]
                            }
                        )
                    except ClientError as e:
                        print(f"Unable to update table data: {e.response['Error']['Message']}")

        # Remove the message if it succeeded, else retry
        if total_success:
            message[0].delete()

