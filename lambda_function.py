import contextlib
import datetime
import json
import uuid
import os

import boto3

from rds_models import Device, Group

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

DB_ITEMS = {
    "endpoint": os.environ["sqlalchemy_db_endpoint"],
    "auth": os.environ["sqlalchemy_db_auth"],
    "name": os.environ["sqlalchemy_db_name"]
}

engine = create_engine(("mysql+pymysql://"
                        "{auth}@{endpoint}/{name}").format(**DB_ITEMS))
Session = sessionmaker(bind=engine)
session = Session()

BUCKET_NAME = "noticast-messages"
DEFAULT_VOICE = "Salli"

polly = boto3.client('polly')
s3 = boto3.client('s3')
bucket = boto3.resource('s3').Bucket(BUCKET_NAME)
iot = boto3.client('iot-data')


def lambda_handler(event, context):
    json_data = json.loads(event["body"])
    devices = []

    # Check whether `target` is Device or Group {{{
    arn = json_data["target"]
    item = session.query(Device).filter_by(arn=arn).first()
    if item is not None:
        # Found a device
        devices.append(item)
    else:
        # Check if `target` is Group
        group = session.query(Group).filter_by(arn=arn).first()
        if group is not None:
            devices.extend(group.devices)
    # }}}

    # Generate mp3 file {{{
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
    # }}}

    # Publish notification {{{
    will_publish = True
    ctx = event.get("requestContext")
    if ctx is not None:
        if ctx.get("stage") in ("test", "testing", "dev"):
            if json_data.get("testing"):
                will_publish = False

    payload = json.dumps({
        "message": json_data["message"],
        "uri": s3.generate_presigned_url("get_object", Params={
            "Bucket": BUCKET_NAME,
            "Key": output
        }),
        "published": will_publish
    })
    args = {
        "topic": "noticast-messages",
        "payload": payload
    }

    if will_publish:
        iot.publish(**args)
        for device in devices:
            args.update(topic=device.arn)
            iot.publish(**args)

    # }}}

    return {
        "statusCode": 200,
        "body": json.dumps(payload)
    }
