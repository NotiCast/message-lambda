import json
import boto3


def lambda_handler(event, context):
    # TODO implement
    client = boto3.client('sns')
    json_data = json.loads(event["body"])
    args = {
        "TopicArn": "arn:aws:sns:us-east-2:622257978701:play-message",
        "Message": json_data["message"],
    }
    client.publish(**args)
    return {
        "statusCode": 200,
        "body": "yay! sendt messages."
    }
