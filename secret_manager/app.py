import json
import boto3
from botocore.exceptions import ClientError


def get_secret():

    secret_name = "dev/test"
    region_name = "us-east-2"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    secret = get_secret_value_response['SecretString']

    return secret

def create_env_file(secret_json, template, output):
    secret = json.loads(secret_json)

    with open(template, 'r', encoding='utf-8') as f:
        template = f.read()

    for key, value in secret.items():
        placeholder = f"{{{{{key}}}}}"
        template = template.replace(placeholder, str(value))

    with open(output, 'w', encoding='utf-8') as f:
        f.write(template)

if __name__ == "__main__":
    create_env_file(get_secret(), 'env.template', '.env')
