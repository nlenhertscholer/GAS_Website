# run.py
#
# Copyright (C) 2011-2019 Vas Vasiliadis
# University of Chicago
#
# Wrapper script for running AnnTools
#
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

# Extended for Homework 5 by Nesta Lenhert-Scholer

import time
import driver
import boto3
import os
import glob
import shutil
import sys
import json
from configparser import ConfigParser
from botocore.exceptions import ClientError
from botocore.client import Config

# Initialize Config Parser
config = ConfigParser(os.environ)
config.read("ann_config.ini")


class Timer(object):
    """A rudimentary timer for coarse-grained profiling"""
    def __init__(self, verbose=True):
        self.verbose = verbose

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.secs = self.end - self.start
        if self.verbose:
            print(f"Approximate runtime: {self.secs:.2f} seconds")


def upload_file(directory, file, user):
    """Uploads a specified file with key to S3"""

    # Create the object name for the file
    delim = '~'
    try:
        uuid = directory.split('/')[-1]
    except IndexError as e:
        print(e)
        return

    key = uuid + delim + os.path.basename(file)

    bucket = config['aws']['S3ResultsBucket']
    obj_name = f'{config["aws"]["S3KeyPrefix"]}/{user}/{key}'

    # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-uploading-files.html
    try:
        with open(file, "rb") as f:
            s3 = boto3.client("s3", region_name=config['aws']['RegionName'])
            s3.upload_fileobj(f, bucket, obj_name)
        return obj_name     # Return the key to be sent to the database
    except (ClientError, IOError) as e:
        print(e)
        return


def upload_results(directory, user):
    """
        Utility function to format the data to send to upload_file.
        Appends the uuid to the file.annot.vcf path with a delimiting character
    """

    # Get the results file
    try:
        results_file = glob.glob(directory+config['file']['ResultsExtension'])[0]
    except IndexError as e:
        print(e)
        return

    # Upload the file to S3
    return upload_file(directory, results_file, user)


def upload_log(directory, user):
    """
        Utility function to format the data to send to upload_file.
        Appends the uuid to the file.vcf.count.log path with a delimiting character
    """

    # Get the log file
    try:
        results_file = glob.glob(directory+config['file']['LogExtension'])[0]
    except IndexError as e:
        print(e)
        return

    # Upload the file to S3
    return upload_file(directory, results_file, user)


def split_path(path):
    """Extracts the directory path and filename from the path"""

    file = os.path.basename(path)
    directory = os.path.dirname(path)

    return directory, file


if __name__ == '__main__':
    # Call the AnnTools pipeline
    if len(sys.argv) > 1:
        with Timer():
            driver.run(sys.argv[1], 'vcf')

        # Get the job_id, user_id, and completion times
        job_id = sys.argv[2]
        user_id = sys.argv[3]
        complete_time = int(time.time())

        # Get the unique_id and filename
        folder, filename = split_path(sys.argv[1])

        # 1. Upload the results file
        results_key = upload_results(folder, user_id)

        # 2. Upload the log file
        log_key = upload_log(folder, user_id)

        # Update the database with the relevant information
        table = config['aws']['DynamoTable']
        bucket = config['aws']['S3ResultsBucket']

        # Added due to feedback from HW4
        try:
            db = boto3.resource('dynamodb', region_name=config['aws']['RegionName'])
            table = db.Table(table)
            table.update_item(
                Key={
                    'job_id': job_id
                },
                UpdateExpression='SET s3_results_bucket = :val1, '
                                 's3_key_result_file = :val2, '
                                 's3_key_log_file = :val3, '
                                 'job_status = :val4, '
                                 'complete_time = :val5',
                ExpressionAttributeValues={
                    ':val1': bucket,
                    ':val2': results_key,
                    ':val3': log_key,
                    ':val4': "COMPLETED",
                    ':val5': complete_time,
                }
            )
        except ClientError as e:
            print(f"Unable to update database with finalized results: "
                  f"{e.response['Error']['Message']}")

        # 3. Clean up (delete) local job files
        # https://docs.python.org/3/library/shutil.html#shutil.rmtree
        try:
            shutil.rmtree(folder, ignore_errors=True)
        except OSError as e:
            print("Unable to delete job files: ", e)

        # 4. Push notification that job is done
        # Also push notification to archive job
        data = {"user_id": user_id,
                "input_file_name": filename,
                "job_id": job_id,
                "results_file": results_key}

        data_json = json.dumps(data)
        sns_results_topic = config['aws']['SNSResultsTopic']
        sns_archive_topic = config['aws']['SNSArchiveTopic']
        try:
            sns = boto3.client('sns', region_name=config['aws']["RegionName"])
            _ = sns.publish(TopicArn=sns_results_topic, Message=data_json)
            _ = sns.publish(TopicArn=sns_archive_topic, Message=data_json)
        except ClientError as e:
            print(f"Unable to push notification that job is done: "
                  f"{e.response['Error']['Message']}")

    else:
        print("A valid .vcf file must be provided as input to this program.")

# EOF
