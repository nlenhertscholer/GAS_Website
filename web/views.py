# views.py
#
# Copyright (C) 2011-2020 Vas Vasiliadis
# University of Chicago
#
# Application logic for the GAS
#
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import uuid
import time
import json
import logging
from datetime import datetime
import stripe
import io

import boto3
from boto3.dynamodb.conditions import Key
from botocore.client import Config
from botocore.exceptions import ClientError

from flask import (abort, flash, redirect, render_template,
                   request, session, url_for, jsonify)

from gas import app, db
from decorators import authenticated, is_premium
from auth import get_profile, update_profile

"""Start annotation request
Create the required AWS S3 policy document and render a form for
uploading an annotation input file using the policy document.

Note: You are welcome to use this code instead of your own
but you can replace the code below with your own if you prefer.
"""


def process_key(key):
    """Split the key retrieved from the URL into the job_id,
    user_id, and file name"""

    split_key = key.split("/")
    try:
        user = split_key[1]
        uid = split_key[2].split('~')[0]
        file = split_key[2].split('~')[1]
        return user, uid, file
    except IndexError:
        raise


def upload_to_db(data):
    """Function takes data and uploads it to the DynamoDB database"""

    table = app.config["AWS_DYNAMODB_ANNOTATIONS_TABLE"]

    # Connect to the database and upload the current data
    # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/dynamodb.html#using-an-existing-table
    # Used both "Using an existing table" and "Creating a new item"

    # Added error handling due to HW4 feedback
    try:
        db = boto3.resource('dynamodb', region_name=app.config["AWS_REGION_NAME"])
        table = db.Table(table)
        table.put_item(Item=data)
    except ClientError:
        raise


def create_presigned_url(bucket_name, object_name,
                         expiration=app.config['AWS_SIGNED_REQUEST_EXPIRATION']):

    """Creates a presigned url given the bucket and object names
       Source for learning how to do this from
       https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-presigned-urls.html"""

    s3 = boto3.client('s3', region_name=app.config["AWS_REGION_NAME"])
    try:
        response = s3.generate_presigned_url(app.config['AWS_S3_CLIENTMETHOD'],
                                             Params={'Bucket': bucket_name,
                                                     'Key': object_name},
                                             ExpiresIn=expiration)
    except ClientError as e:
        logging.error(e)
        raise

    return response


@app.route('/annotate', methods=['GET'])
@authenticated
def annotate():
    # Create a session client to the S3 service
    s3 = boto3.client('s3',
                      region_name=app.config['AWS_REGION_NAME'],
                      config=Config(signature_version='s3v4'))

    bucket_name = app.config['AWS_S3_INPUTS_BUCKET']
    user_id = session['primary_identity']

    # Generate unique ID to be used as S3 key (name)
    key_name = app.config['AWS_S3_KEY_PREFIX'] + user_id + '/' + \
        str(uuid.uuid4()) + '~${filename}'

    # Create the redirect URL
    redirect_url = str(request.url) + '/job'

    # Define policy fields/conditions
    encryption = app.config['AWS_S3_ENCRYPTION']
    acl = app.config['AWS_S3_ACL']
    fields = {
        "success_action_redirect": redirect_url,
        "x-amz-server-side-encryption": encryption,
        "acl": acl
    }
    conditions = [
        ["starts-with", "$success_action_redirect", redirect_url],
        {"x-amz-server-side-encryption": encryption},
        {"acl": acl}
    ]

    # Generate the presigned POST call
    try:
        presigned_post = s3.generate_presigned_post(
            Bucket=bucket_name,
            Key=key_name,
            Fields=fields,
            Conditions=conditions,
            ExpiresIn=app.config['AWS_SIGNED_REQUEST_EXPIRATION'])
    except ClientError as e:
        app.logger.error(f"Unable to generate presigned URL for upload: {e}")
        return abort(500)

    # Render the upload form which will parse/submit the presigned POST
    return render_template('annotate.html', s3_post=presigned_post)


@app.route('/annotate/job', methods=['GET'])
@authenticated
def create_annotation_job_request():
    """Fires off an annotation job
    Accepts the S3 redirect GET request, parses it to extract
    required info, saves a job item to the database, and then
    publishes a notification for the annotator service.

    Note: Update/replace the code below with your own from previous
    homework assignments
    """

    # Get bucket name, key, and job ID from the S3 redirect URL
    bucket_name = str(request.args.get('bucket'))
    s3_key = str(request.args.get('key'))

    # Extract the job ID from the S3 key
    try:
        user_id, job_id, input_file_name = process_key(s3_key)
    except IndexError as e:
        logging.error(e)
        return abort(500)

    # Create a job item and persist it to the annotations database
    data = {"job_id": job_id,
            "user_id": user_id,
            "input_file_name": input_file_name,
            "s3_inputs_bucket": bucket_name,
            "s3_key_input_file": s3_key,
            "submit_time": int(time.time()),
            "job_status": "PENDING",
            }

    # Persist job to database
    try:
        upload_to_db(data)
    except ClientError as e:
        logging.error(e)
        code = e.response['ResponseMetadata']['HTTPStatusCode']
        abort(code)

    # Send message to request queue
    data_json = json.dumps(data)
    sns_topic = app.config["AWS_SNS_JOB_REQUEST_TOPIC"]
    try:
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns.html#SNS.Topic.publish
        sns = boto3.client('sns', region_name=app.config["AWS_REGION_NAME"])
        msg = sns.publish(TopicArn=sns_topic, Message=data_json)
    except ClientError as e:
        logging.error(e)
        code = e.response['ResponseMetadata']['HTTPStatusCode']
        abort(code)

    return render_template('annotate_confirm.html', job_id=job_id)


@app.route('/annotations', methods=['GET'])
@authenticated
def annotations_list():
    """List all annotations for the user
    """

    # Obtain the user id
    user_id = session['primary_identity']

    # Query the database
    index = app.config["AWS_DYNAMODB_USERID_INDEX"]
    key = app.config["AWS_DYNAMODB_USER_TABLE_KEY"]
    table = app.config["AWS_DYNAMODB_ANNOTATIONS_TABLE"]
    try:
        db = boto3.resource('dynamodb', region_name=app.config["AWS_REGION_NAME"])
        table = db.Table(table)
        results = table.query(
            IndexName=index,
            KeyConditionExpression=Key(key).eq(user_id)
        )
    except ClientError as e:
        logging.error(e)
        code = e.response['ResponseMetadata']['HTTPStatusCode']
        abort(code)

    info = results["Items"]

    # Convert Request time to local time
    for item in info:
        item["submit_time"] = time.strftime('%Y-%m-%d %H:%M',
                                            time.localtime(item["submit_time"]))

    return render_template('annotations.html', annotations=info)


@app.route('/annotations/<id>', methods=['GET'])
@authenticated
def annotation_details(id):
    """Display details of a specific annotation job
    """

    # Query the database
    key = app.config["AWS_DYNAMODB_PRIMARY_KEY"]
    table = app.config["AWS_DYNAMODB_ANNOTATIONS_TABLE"]

    try:
        db = boto3.resource('dynamodb', region_name=app.config["AWS_REGION_NAME"])
        table = db.Table(table)
        results = table.query(
            KeyConditionExpression=Key(key).eq(id)
        )
    except ClientError as e:
        logging.error(e)
        code = e.response['ResponseMetadata']['HTTPStatusCode']
        abort(code)

    try:
        info = results["Items"][0]
    except IndexError:
        # No items were returned
        abort(404)

    # Check that this user is authorized
    if info["user_id"] != session["primary_identity"]:
        abort(403)

    # Generate Presigned URL so user can download input and results file
    try:
        info['input_file_url'] = create_presigned_url(info['s3_inputs_bucket'],
                                                      info['s3_key_input_file'])
        info['result_file_url'] = create_presigned_url(info['s3_results_bucket'],
                                                       info['s3_key_result_file'])
    except ClientError as e:
        code = e.response['ResponseMetadata']['HTTPStatusCode']
        abort(code)

    # Convert to local time
    info["submit_time"] = time.strftime('%Y-%m-%d %H:%M',
                                        time.localtime(info["submit_time"]))
    info["complete_time"] = time.strftime('%Y-%m-%d %H:%M',
                                          time.localtime(info["complete_time"]))

    # Render HTML page
    return render_template('annotation_details.html', annotation=info)


@app.route('/annotations/<id>/log', methods=['GET'])
@authenticated
def annotation_log(id):
    """Display the log file contents for an annotation job
    """

    # Query the database
    key = app.config["AWS_DYNAMODB_PRIMARY_KEY"]
    table = app.config["AWS_DYNAMODB_ANNOTATIONS_TABLE"]

    try:
        db = boto3.resource('dynamodb', region_name=app.config["AWS_REGION_NAME"])
        table = db.Table(table)
        results = table.query(
            KeyConditionExpression=Key(key).eq(id)
        )
    except ClientError as e:
        logging.error(e)
        code = e.response['ResponseMetadata']['HTTPStatusCode']
        abort(code)

    try:
        info = results["Items"][0]
    except IndexError:
        abort(404)

    try:
        # Read the log file directly from S3 and load it into memory
        # https://stackoverflow.com/a/48696641
        s3 = boto3.resource('s3', app.config["AWS_REGION_NAME"])
        s3_results_bucket = s3.Bucket(app.config["AWS_S3_RESULTS_BUCKET"])
        log_file = s3_results_bucket.Object(info['s3_key_log_file'])
        # Read it into a ByteIO object
        raw_input = io.BytesIO(log_file.get()['Body'].read())

    except ClientError as e:
        code = e.response['ResponseMetadata']['HTTPStatusCode']
        abort(code)

    # Render the template with decoded text
    return render_template('view_log.html', job_id=id,
                           log_file_contents=raw_input.read().decode("utf-8"))


@app.route('/subscribe', methods=['GET', 'POST'])
@authenticated
def subscribe():
    """Subscription management handler
    """
    if request.method == 'GET':
        # Display form to get subscriber credit card info
        if session.get('role') == "free_user":
            return render_template('subscribe.html')
        else:
            return redirect(url_for('profile'))

    elif request.method == 'POST':
        # Process the subscription request
        token = str(request.form['stripe_token']).strip()

        # Create a customer on Stripe
        stripe.api_key = app.config['STRIPE_SECRET_KEY']
        try:
            customer = stripe.Customer.create(
                card=token,
                plan="premium_plan",
                email=session.get('email'),
                description=session.get('name')
            )
        except Exception as e:
            app.logger.error(f"Failed to create customer billing record: {e}")
            return abort(500)

        # Update user role to allow access to paid features
        update_profile(
            identity_id=session['primary_identity'],
            role="premium_user"
        )

        # Update role in the session
        session['role'] = "premium_user"

        # Request restoration of the user's data from Glacier
        # Add code here to initiate restoration of archived user data
        # Make sure you handle files not yet archived!

        # Display confirmation page
        return render_template('subscribe_confirm.html',
                               stripe_customer_id=str(customer['id']))


@app.route('/unsubscribe', methods=['GET'])
@authenticated
def unsubscribe():
    """Reset subscription
    """
    # Hacky way to reset the user's role to a free user; simplifies testing
    update_profile(
        identity_id=session['primary_identity'],
        role="free_user"
    )
    return redirect(url_for('profile'))


"""DO NOT CHANGE CODE BELOW THIS LINE
*******************************************************************************
"""


@app.route('/', methods=['GET'])
def home():
    """Home page
    """
    return render_template('home.html')


@app.route('/login', methods=['GET'])
def login():
    """Login page; send user to Globus Auth
    """
    app.logger.info(f"Login attempted from IP {request.remote_addr}")
    # If user requested a specific page,
    # save it session for redirect after auth
    if request.args.get('next'):
        session['next'] = request.args.get('next')
    return redirect(url_for('authcallback'))


@app.errorhandler(404)
def page_not_found(e):
    """404 error handler
    """
    return render_template('error.html',
                           title='Page not found', alert_level='warning',
                           message="The page you tried to reach does not exist. \
      Please check the URL and try again."
                           ), 404


@app.errorhandler(403)
def forbidden(e):
    """403 error handler
    """
    return render_template('error.html',
                           title='Not authorized', alert_level='danger',
                           message="You are not authorized to access this page. \
      If you think you deserve to be granted access, please contact the \
      supreme leader of the mutating genome revolutionary party."
                           ), 403


@app.errorhandler(405)
def not_allowed(e):
    """405 error handler
    """
    return render_template('error.html',
                           title='Not allowed', alert_level='warning',
                           message="You attempted an operation that's not allowed; \
      get your act together, hacker!"
                           ), 405


@app.errorhandler(500)
def internal_error(error):
    """500 error handler
    """
    return render_template('error.html',
                           title='Server error', alert_level='danger',
                           message="The server encountered an error and could \
      not process your request."
                           ), 500

### EOF
