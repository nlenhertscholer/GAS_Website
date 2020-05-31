# notify.py
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
import psycopg2
import json
from configparser import ConfigParser

# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers

# Get configuration
config = ConfigParser(os.environ)
config.read("notify_config.ini")

# Connect to SQS and get the message queue
# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Queue
queue_url = config['aws']['SQSResultsURL']
sqs = boto3.resource("sqs", region_name=config['aws']['RegionName'])
queue = sqs.Queue(queue_url)

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

        # Extract parameters from message
        user_id = input_data['user_id']
        job_id = input_data['job_id']

        # Retrieve user information and begin formatting email
        success = True
        try:
            profile = helpers.get_user_profile(user_id)
        except (ClientError, psycopg2.Error) as e:
            print(f"Unable to retrieve user details: {e.response['Error']['Message']}")
            success = False

        if success:
            recipient = profile['email']
            name = profile['name']
            url = config['web']['BaseURL'] + job_id
            subject = "GAS Job Annotation Completed"
            body = f'Hello {name},<br/><br/>' \
                   f'Your annotation job ' \
                   f'<a class="link" href="{url}" target="_blank">{job_id}</a> ' \
                   f'has been completed.<br/><br/>' \
                   f'Thank you for using our GAS Platform,<br/>' \
                   f'Genomics Annotation Service'

            # Send the email
            try:
                response = helpers.send_email_ses(recipient, subject=subject, body=body)
            except ClientError as e:
                print(f"Unable to send email: {e.response['Error']['Message']}")
                success = False

            # Delete the message if it was sent successfully
            if success and response["ResponseMetadata"]['HTTPStatusCode'] == 200:
                # Print out to keep a log
                print(f"Email sent about Job ID: {job_id}")
                # Delete the message from the queue,
                # if job was successfully submitted
                message[0].delete()
