import json
import boto3
import botocore
from datetime import datetime


client = boto3.client('stepfunctions')



def lambda_handler(event, context):
    
    now = datetime.now()
    
    # execution name
    executionName = "retail-ETL-stepfunction-" + now.strftime("%d-%m-%Y-%H-%M-%S")
    
    # list step function status 
    sfExecution = client.list_executions(stateMachineArn='arn:aws:states:us-east-1:471538592417:stateMachine:retail-ETL',
        statusFilter='RUNNING',
        maxResults=1)
 
    # run new execution if not running.
    if not sfExecution["executions"]:
        print(sfExecution)
        response = client.start_execution(stateMachineArn='arn:aws:states:us-east-1:471538592417:stateMachine:retail-ETL', name=executionName) 
       
        return "SUCCESS: Step Function executed"
    else :
        return "FAILURE: Step Function is already Running"

