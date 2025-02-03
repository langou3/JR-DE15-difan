import boto3
import os
import json
from datetime import datetime
import time

def lambda_handler(event, context):
    try:
        sagemaker = boto3.client('sagemaker')
        
        # Get environment variables
        notebook_name = os.environ['NOTEBOOK_NAME']
        
        # Get notebook instance status
        response = sagemaker.describe_notebook_instance(
            NotebookInstanceName=notebook_name
        )
        
        current_status = response['NotebookInstanceStatus']
        
        # Start notebook if it's stopped
        if current_status == 'Stopped':
            print(f"Starting notebook instance {notebook_name}")
            sagemaker.start_notebook_instance(
                NotebookInstanceName=notebook_name
            )
            
            # Wait for notebook to be in service (max 10 minutes)
            max_wait = 600
            wait_time = 0
            while wait_time < max_wait:
                response = sagemaker.describe_notebook_instance(
                    NotebookInstanceName=notebook_name
                )
                if response['NotebookInstanceStatus'] == 'InService':
                    break
                time.sleep(30)
                wait_time += 30
                
            if wait_time >= max_wait:
                raise Exception("Notebook instance failed to start in time")
        
        # Execute the batch prediction notebook
        # Note: You need to have the prediction notebook set up in the notebook instance
        response = sagemaker.create_presigned_notebook_instance_url(
            NotebookInstanceName=notebook_name,
            SessionExpirationDurationInSeconds=7200
        )
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Batch prediction process initiated',
                'notebookStatus': current_status,
                'notebookUrl': response['AuthorizedUrl']
            })
        }
    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': f'Error initiating batch prediction: {str(e)}'
            })
        } 