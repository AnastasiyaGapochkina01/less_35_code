import json
from datetime import datetime

def lambda_handler(event, context):
    current_time = datetime.now().strftime("%H:%M:%S")
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': f'Текущее время: {current_time}'
        })
    }
