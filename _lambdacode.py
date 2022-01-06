import boto3
import gzip
import json
import logging
import os
from io import StringIO

firehose = boto3.client('firehose')


def lambda_handler(event, context):
    encodedLogsData = str(event['awslogs']['data'])
    decodedLogsData = gzip.GzipFile(fileobj=StringIO(encodedLogsData.decode('base64', 'strict'))).read()
    allEvents = json.loads(decodedLogsData)

    records = []

    for event in allEvents['logEvents']:

        logEvent = {
            'Data': str(event['message']) + "\n"
        }

        records.insert(len(records), logEvent)

        if len(records) > 499:
            firehose.put_record_batch(
                DeliveryStreamName=os.environ['DELIVERY_STREAM_NAME'],
                Records=records
            )

            records = []

    if len(records) > 0:
        firehose.put_record_batch(
            DeliveryStreamName=os.environ['DELIVERY_STREAM_NAME'],
            Records=records
        )

if __name__ == '__main__':
    lambda_handler()
    input = {"awslogs" : {"data": {2 unknown eni-09a44dfd0ebd78288 - - - - - - - 1638859102 1638859154 - NODATA}}}