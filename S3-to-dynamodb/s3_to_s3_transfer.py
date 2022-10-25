from __future__ import print_function
import boto3
import time, urllib
import json


s3 = boto3.client('s3')

def lambda_handler(event, context):
    print(event)
    source_bucket = event['Records'][0]['s3']['bucket']['name']
    print(source_bucket)
    object_key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])
    target_bucket = 'cdeekfors3'
    copy_source = {'Bucket': source_bucket, 'Key': object_key}
    print(copy_source)
    object_key  = object_key.split("/")[-1]
    prefix = 'name/'
    object_key = prefix+object_key
    print(object_key)
    print ("Source bucket : ", source_bucket)
    print ("Target bucket : ", target_bucket)
    print ("Log Stream name: ", context.log_stream_name)
    print ("Log Group name: ", context.log_group_name)
    print ("Request ID: ", context.aws_request_id)
    print ("Mem. limits(MB): ", context.memory_limit_in_mb)
    try:
        print ("Using waiter to waiting for object to persist through s3 service")
        print(target_bucket,object_key)
        s3.copy_object(Bucket=target_bucket, Key=object_key, CopySource=copy_source)
        print('copied')
    except Exception as err:
        print ("Error -"+str(err))