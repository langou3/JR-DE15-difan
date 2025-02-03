import boto3
import os
import json
from datetime import datetime

def get_latest_model_path(s3_client, bucket, prefix='output/'):
    """Get the latest model.tar.gz file from the specified S3 bucket and prefix"""
    try:
        # List objects in the bucket with the given prefix
        response = s3_client.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix
        )
        
        if 'Contents' not in response:
            raise Exception("No models found in the specified location")
            
        # Filter for model.tar.gz files and get the latest one
        model_files = [
            obj for obj in response['Contents']
            if obj['Key'].endswith('/output/model.tar.gz')
        ]
        
        if not model_files:
            raise Exception("No model.tar.gz files found")
            
        # Sort by last modified time and get the latest
        latest_model = sorted(model_files, key=lambda x: x['LastModified'], reverse=True)[0]
        return latest_model['Key']
    except Exception as e:
        print(f"Error finding latest model: {str(e)}")
        raise e

def lambda_handler(event, context):
    try:
        sagemaker = boto3.client('sagemaker')
        s3 = boto3.client('s3')
        sts = boto3.client('sts')
        
        # Get account and region
        account = sts.get_caller_identity()['Account']
        region = os.environ.get('AWS_REGION')
        
        # Get environment variables
        curated_zone_bucket = os.environ['CURATED_ZONE_BUCKET']
        consumption_zone_bucket = os.environ['CONSUMPTION_ZONE_BUCKET']
        training_role_arn = os.environ['SAGEMAKER_ROLE_ARN']
        
        # Get the latest model path
        latest_model_path = get_latest_model_path(s3, consumption_zone_bucket)
        
        # Configure training job
        timestamp = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
        training_job_name = f"training-job-{timestamp}"
        
        # Start training job
        response = sagemaker.create_training_job(
            TrainingJobName=training_job_name,
            AlgorithmSpecification={
                'TrainingImage': f's3://{consumption_zone_bucket}/{latest_model_path}',
                'TrainingInputMode': 'File'
            },
            RoleArn=training_role_arn,
            InputDataConfig=[
                {
                    'ChannelName': 'training',
                    'DataSource': {
                        'S3DataSource': {
                            'S3DataType': 'S3Prefix',
                            'S3Uri': f's3://{curated_zone_bucket}/curated/',
                            'S3DataDistributionType': 'FullyReplicated'
                        }
                    }
                }
            ],
            ResourceConfig={
                'InstanceType': 'ml.m5.xlarge',
                'InstanceCount': 1,
                'VolumeSizeInGB': 30,
                'UseSpotInstances': True,
                'MaxWaitTimeInSeconds': 86400,  # 24 hours
                'MaxRuntimeInSeconds': 43200    # 12 hours
            },
            StoppingCondition={
                'MaxRuntimeInSeconds': 43200  # 12 hours
            },
            OutputDataConfig={
                'S3OutputPath': f's3://{consumption_zone_bucket}/model-output/'
            }
        )
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Training job started successfully',
                'jobName': training_job_name,
                'modelPath': latest_model_path
            })
        }
    except Exception as e:
        print(f"Error: {str(e)}")
        raise e