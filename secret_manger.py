import boto3
from botocore.exceptions import ClientError
import json

def get_secret(secret_name, region_name):
    client = boto3.client('secretsmanager', region_name=region_name)

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        print(f"Ошибка при получении секрета: {e}")
        return None
    if 'SecretString' in get_secret_value_response:
        secret = get_secret_value_response['SecretString']
    else:
        secret = get_secret_value_response['SecretBinary']

    try:
        secret_dict = json.loads(secret)
        return secret_dict
    except json.JSONDecodeError:
        return secret


if __name__ == "__main__":
    secret_name = "my/secret/name"  
    region_name = "us-east-1"        
    secret = get_secret(secret_name, region_name)
    if secret:
        print("Полученный секрет:", secret)
