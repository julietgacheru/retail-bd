import json
import boto3
import botocore

client = boto3.client('glue')

def lambda_handler(event, context):
    try:
        dept_summ = client.start_crawler(Name = 'dept_summ')
        store_summ = client.start_crawler(Name = 'store_summ')
        vendor_summ = client.start_crawler(Name = 'vendor_summ')
  
    except botocore.exceptions.ClientError as error:
        if error.response['Error']['Code'] == 'CrawlerRunningException':
          return error.response["Message"]
           
        else:
            return "problem starting crawler"


