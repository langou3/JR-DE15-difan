from crhelper import CfnResource
import boto3
import logging

logger = logging.getLogger(__name__)
helper = CfnResource(json_logging=True, log_level='INFO')

def create_notification(event, bucket_name, lambda_arn, prefix='data/'):
    """Configure S3 bucket notification"""
    try:
        s3 = boto3.client('s3')
        
        # Log the current configuration
        current_config = s3.get_bucket_notification_configuration(Bucket=bucket_name)
        logger.info(f"Current notification config: {current_config}")
        
        notification_config = {
            'LambdaFunctionConfigurations': [
                {
                    'LambdaFunctionArn': lambda_arn,
                    'Events': ['s3:ObjectCreated:*', 's3:ObjectRemoved:*'],
                    'Filter': {
                        'Key': {
                            'FilterRules': [
                                {
                                    'Name': 'prefix',
                                    'Value': prefix
                                }
                            ]
                        }
                    }
                }
            ]
        }
        
        # Log the configuration we're trying to apply
        logger.info(f"Attempting to apply notification config: {notification_config}")
        
        s3.put_bucket_notification_configuration(
            Bucket=bucket_name,
            NotificationConfiguration=notification_config
        )
        
        # Verify the configuration was applied correctly
        new_config = s3.get_bucket_notification_configuration(Bucket=bucket_name)
        logger.info(f"New notification config after update: {new_config}")
        
        return True
    except Exception as e:
        logger.error(f"Error configuring notifications: {str(e)}")
        logger.error(f"Error details: {type(e).__name__}")
        raise

def delete_notification(bucket_name):
    """Delete S3 bucket notification configuration"""
    try:
        s3 = boto3.client('s3')
        s3.put_bucket_notification_configuration(
            Bucket=bucket_name,
            NotificationConfiguration={}
        )
        logger.info(f"Successfully removed notifications from bucket {bucket_name}")
        return True
    except Exception as e:
        logger.error(f"Error removing notifications: {str(e)}")
        raise

@helper.create
def create(event, context):
    """Handle create event"""
    props = event['ResourceProperties']
    bucket_name = props.get('BucketName')
    lambda_arn = props.get('LambdaArn')
    prefix = props.get('Prefix', 'data/')
    
    if not bucket_name or not lambda_arn:
        raise ValueError("BucketName and LambdaArn are required parameters")
    
    return create_notification(event, bucket_name, lambda_arn, prefix)

@helper.update
def update(event, context):
    """Handle update event"""
    props = event['ResourceProperties']
    bucket_name = props.get('BucketName')
    lambda_arn = props.get('LambdaArn')
    prefix = props.get('Prefix', 'data/')
    
    if not bucket_name or not lambda_arn:
        raise ValueError("BucketName and LambdaArn are required parameters")
    
    return create_notification(event, bucket_name, lambda_arn, prefix)

@helper.delete
def delete(event, context):
    """Handle delete event"""
    props = event['ResourceProperties']
    bucket_name = props.get('BucketName')
    
    if not bucket_name:
        raise ValueError("BucketName is required for deletion")
    
    return delete_notification(bucket_name)

def handler(event, context):
    """Lambda handler function"""
    helper(event, context)
