import boto3
import json
from botocore.errorfactory import ClientError
import os

glue = boto3.client('glue')
s3 = boto3.client('s3')

def get_mappings_by_version(context):
  input_tables = os.environ["GLUE_INPUT_TABLES"].split(',')
  mappings = {}
  for table in input_tables:
    print('Getting ' + table + ' column mappings from S3')
    obj = s3.get_object(Bucket='student-data-pipeline', 
      Key=os.environ['ENVIRONMENT'] + '/mappings/' + table + '/column_mappings.json')
    mappings[table] = json.load(obj['Body'])
  return mappings

def lambda_handler(event, context):
  mappings = get_mappings_by_version(context)
  glue_job_args = {
    '--mappings_by_version': json.dumps(mappings, indent=2)
  }
  try:
    response = glue.start_job_run(
                 JobName = os.environ['GLUE_JOB_NAME'],
                 Arguments = glue_job_args)
    print('Started the ETL job')
  except ClientError as e:
    if e.response['Error']['Code'] == 'ConcurrentRunsExceededException':
      print('Job already running')
    raise e
