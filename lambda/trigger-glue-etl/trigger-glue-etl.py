import boto3
from botocore.errorfactory import ClientError
import os

glue = boto3.client('glue')
sqs = boto3.client('sqs')

def lambda_handler(event, context):
  try:
    response = glue.start_crawler(
                 Name = os.environ['GLUE_CRAWLER_NAME'])
    print('Started the Glue crawler')
    try:
      response = sqs.purge_queue(
        QueueUrl=os.environ['QUEUE_URL']
      )
      print('Purged the queue')
    except ClientError as e:
      print('Could not purge the queue', e)
  except ClientError as e:
    if e.response['Error']['Code'] == 'ConcurrentRunsExceededException':
      print('Job already running')
    raise e
