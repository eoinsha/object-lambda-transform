import logging
import os
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd
import requests
import botocore
import boto3

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3')
bucket_name = os.environ['BUCKET_NAME']


def handle_event(event, context):
    """
    S3 Object Lambda handler. Performs on-the-fly conversion of CSV to Parquet
    """
    logger.info(event)

    get_obj_ctx = event['getObjectContext']
    request_route = get_obj_ctx['outputRoute']
    request_token = get_obj_ctx['outputToken']
    obj_url = get_obj_ctx['inputS3Url']
    requested_url = event['userRequest']['url']
    path = Path(urlparse(requested_url).path).relative_to('/')

    response = requests.get(obj_url)
    resp = {'StatusCode': response.status_code}

    if response.status_code == 404 and path.suffix == '.parquet':
        # Load CSV and convert to Parquet.
        csv_key = str(path.with_suffix('.csv'))
        try:
            csv_body = s3_client.get_object(Bucket=bucket_name, Key=csv_key)['Body']
            resp['Body'] = pd.read_csv(csv_body).to_parquet()
            resp['StatusCode'] = 200
        except botocore.exceptions.ClientError as error:
            resp['ErrorCode'] = error.response['Error']['Code']
            resp['StatusCode'] = error.response['ResponseMetadata']['HTTPStatusCode']
            resp['ErrorMessage'] = error.response['Error']['Message']
    else:
        resp['Body'] = response.content

    s3_client.write_get_object_response(
        RequestRoute=request_route,
        RequestToken=request_token,
        **resp
    )

    return {'status_code': 200}
