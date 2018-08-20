# vim:set et sw=4 ts=4 foldmethod=marker:
import contextlib
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

BUCKET_NAME = os.environ["messages_bucket"]
DEFAULT_VOICE = "Salli"

polly = boto3.client('polly')
s3 = boto3.client('s3')
bucket = boto3.resource('s3').Bucket(BUCKET_NAME)
iot = boto3.client('iot-data')


def http_error_from_exception(exc):
    return {"statusCode": 400, "body": json.dumps({
            "message": str(exc), "type": str(type(exc))})}


def mail_error(exc):
    # can't really do much considering the callable is a relay
    return


def lambda_handler(event, _context):
    if "body" in event:  # HTTP API
        try:
            json_data = json.loads(event["body"])
            arn = json_data["target"]
            text = json_data["message"]
            return {
                "statusCode": 200,
                "body": json.dumps(send_message(
                        arn, text, voice_id=json_data.get("voice_id",
                                                          DEFAULT_VOICE)))
            }
        except json.decoder.JSONDecodeError as e:
            return http_error_from_exception(e)
        except KeyError as e:
            return http_error_from_exception(e)
    elif "Records" in event:  # E-Mail API
        print(event)
    else:
        print(event)
        print("== Unknown Event ==")


def send_message(arn, text, voice_id=DEFAULT_VOICE):
    devices = []

    # Check whether `target` is Device or Group {{{
    is_group = False
    item = session.query(Device).filter_by(arn=arn).first()
    if item is not None:
        # Found a device
        devices.append(item)
    else:
        # Check if `target` is Group
        group = session.query(Group).filter_by(arn=arn).first()
        if group is not None:
            is_group = True
            devices.extend(group.devices)
    # }}}

    # Generate mp3 file {{{
    response = polly.synthesize_speech(
        OutputFormat="mp3",
        Text=text,
        TextType="text",
        VoiceId=voice_id)
    output = str(uuid.uuid4()) + ".mp3"

    if "AudioStream" in response:
        stream = response["AudioStream"]
        with contextlib.closing(stream):
            output_file = bucket.Object(output)
            output_file.put(Body=stream.read())
    # }}}

    # Publish notification {{{
    payload = {
        "message": text,
        "uri": s3.generate_presigned_url("get_object", Params={
            "Bucket": BUCKET_NAME,
            "Key": output
        })
    }
    args = {
        "topic": "noticast-messages",
        "payload": json.dumps(payload),
        "qos": 1
    }

    iot.publish(**args)
    for device in devices:
        args.update(topic=device.arn)
        iot.publish(**args)
    # }}}

    payload.update(devices=[d.arn for d in devices], is_group=is_group)

    return payload
