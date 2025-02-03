import boto3
import json

def lambda_handler(event, context):
    try:
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table('test_predictions_Torain')
        
        query_params = event.get('queryStringParameters', {})
        user_id = query_params.get('user_id')
        
        if not user_id:
            return {
                'statusCode': 400,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({
                    'error': 'user_id is required'
                })
            }
        
        response = table.query(
            KeyConditionExpression=boto3.dynamodb.conditions.Key('user_id').eq(user_id)
        )
        
        items = response['Items']
        sorted_items = sorted(
            items,
            key=lambda x: float(x.get('predicted_probability', 0)),
            reverse=True
        )
        
        top_3_products = sorted_items[:3]
        
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps(top_3_products)
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'error': str(e)
            })
        } 