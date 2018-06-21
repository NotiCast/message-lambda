import contextlib
import datetime
import json
import uuid

import boto3

BUCKET_NAME = "noticast-messages"
DEFAULT_VOICE = "Salli"

polly = boto3.client('polly')
s3 = boto3.client('s3')
bucket = boto3.resource('s3').Bucket(BUCKET_NAME)
iot = boto3.client('iot-data')


def lambda_handler(event, context):
    json_data = json.loads(event["body"])
    response = polly.synthesize_speech(
        OutputFormat="mp3",
        Text=json_data["message"],
        TextType="text",
        VoiceId=json_data.get("voice_id", DEFAULT_VOICE))
    output = str(uuid.uuid4()) + ".mp3"
    if "AudioStream" in response:
        stream = response["AudioStream"]
        with contextlib.closing(stream):

            output_file = bucket.Object(output)
            output_file.put(Body=stream.read())
    payload = json.dumps({
        "message": json_data["message"],
        "uri": s3.generate_presigned_url("get_object", Params={
            "Bucket": BUCKET_NAME,
            "Key": output
        })
    })
    args = {
        "topic": "noticast-messages",
        "payload": payload
    }
    iot.publish(**args)
    return {
        "statusCode": 200,
        "body": json.dumps(payload)
    }
