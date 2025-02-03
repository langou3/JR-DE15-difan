import boto3
import json
import os
from datetime import datetime, timezone

class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

def handler(event, context):
    glue = boto3.client('glue')
    print(f"Received event: {json.dumps(event, indent=2)}")
    
    try:
        workflow_name = os.environ.get('WORKFLOW_NAME')
        if not workflow_name:
            raise ValueError("WORKFLOW_NAME environment variable is not set")
        
        if not is_valid_s3_event(event):
            print("Event validation failed")
            return {
                'statusCode': 400,
                'body': 'Event validation failed - see CloudWatch logs for details'
            }
        
        # initiate a new workflow run
        print("Starting workflow run")
        workflow_response = glue.start_workflow_run(Name=workflow_name)
        print(f"Workflow response: {json.dumps(workflow_response, indent=2, cls=DateTimeEncoder)}")
        return {
            'statusCode': 200,
            'body': 'Successfully started workflow'
        }
    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': f"Error: {str(e)}"
        }

def is_valid_s3_event(event):
    try:
        print(f"Validating event: {json.dumps(event, indent=2)}")
        
        # check if the event is a valid S3 event
        if 'Records' not in event or not event['Records']:
            print("Event does not contain Records")
            return False
        
        # iterate over all records
        for record in event['Records']:
            # check if the event source is aws:s3
            if record.get('eventSource') != 'aws:s3':
                print(f"Event source is not aws:s3: {record.get('eventSource')}")
                return False
            
            # check if the event type is supported
            if record.get('eventName') not in [
                'ObjectCreated:Put',
                'ObjectCreated:Post',
                'ObjectCreated:Copy',
                'ObjectCreated:CompleteMultipartUpload',
                'ObjectRemoved:Delete',
                'ObjectRemoved:DeleteMarkerCreated'
            ]:
                print(f"Unsupported event type: {record.get('eventName')}")
                return False
            
            # check if the record contains s3 information
            if 's3' not in record:
                print("Record does not contain s3 information")
                return False
                
            s3 = record['s3']
            if 'bucket' not in s3 or 'name' not in s3['bucket']:
                print("Record does not contain bucket name")
                return False
                
            if 'object' not in s3 or 'key' not in s3['object']:
                print("Record does not contain object key")
                return False
            
            # check if the object key starts with the specified prefix
            prefix = os.environ.get('OBJECT_KEY_PREFIX', 'data/')
            if not s3['object']['key'].startswith(prefix):
                print(f"Object key does not start with {prefix}: {s3['object']['key']}")
                return False
            
            print(f"Event validation successful for record: {json.dumps(record, indent=2)}")
        
        print("All records validated successfully")
        return True
        
    except Exception as e:
        print(f"Error validating event: {str(e)}")
        return False
