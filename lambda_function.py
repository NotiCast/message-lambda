# vim:set et sw=4 ts=4 foldmethod=marker:
import contextlib
import json
import uuid
import os
import re

import boto3

from raven import Client

from rds_models import Device, Group

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

raven = Client(os.environ["raven_endpoint"])

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
EMAIL_DOMAIN = os.environ["email_domain"]
DEFAULT_VOICE = "Salli"

polly = boto3.client('polly')
s3 = boto3.client('s3')
bucket = boto3.resource('s3').Bucket(BUCKET_NAME)
iot = boto3.client('iot-data')


class TargetNotFoundError(Exception):
    pass


def http_error_from_exception(exc, status_code=400):
    return {"statusCode": status_code, "body": json.dumps({
            "message": str(exc), "type": str(type(exc))})}


def match_subject(message):
    # Strip out any text containing "Fwd:"
    while True:
        for match in ["Fwd:"]:
            if message[:len(match)] == match:
                message = message[len(match):]
        else:
            # break the While loop after no match is found
            break
    return message


def filter_subject(message):
    # Skip messages with "Re: " spamming users with chains
    for match in ["Re: "]:
        if match in message:
            return False
    return True


# Match ARN in email
email_pattern = re.compile(r'([0-9A-Fa-f]+)@')


# Dispatcher for various incoming requests {{{
def lambda_handler(event, _context):
    if "body" in event:  # HTTP API
        try:
            json_data = json.loads(event["body"])
            arn = json_data["target"]
            text = json_data["message"]
            return {
                "statusCode": 200,
                "body": json.dumps(send_message(
                        arn, text,
                        text_type=json_data.get("message_type", "text"),
                        voice_id=json_data.get("voice_id", DEFAULT_VOICE)))
            }
        except json.decoder.JSONDecodeError as e:
            return http_error_from_exception(e)
        except KeyError as e:
            return http_error_from_exception(e)
        except TargetNotFoundError as e:
            return http_error_from_exception(e, status_code=404)
        except Exception as e:
            body = event["body"]
            raven.user_context(dict(system="json+http"))
            raven.user_context(payload=event["body"])
            try:
                raven.user_context(json_payload=json.loads(body))
            except Exception as e:
                pass
            raven.captureException(e)
    elif "Records" in event:  # E-Mail API
        mail = event["Records"][0]["ses"]["mail"]
        headers = mail["commonHeaders"]
        # Check headers
        arns = []
        subject = match_subject(headers["subject"])
        print("Found subject", subject)
        print("Headers:", headers)
        if not filter_subject(subject):
            return  # Filtered content
        for segment in ["to", "cc"]:
            if segment in headers:
                for address in headers[segment]:
                    print("is:", EMAIL_DOMAIN, "in:", address,
                          EMAIL_DOMAIN in address)
                    if EMAIL_DOMAIN in address:
                        match = email_pattern.search(address)
                        if match is not None:
                            arns.append(match.groups()[0])
        print("Targets:", arns)
        for target in arns:
            try:
                send_message(target, subject)
            except TargetNotFoundError as e:
                pass
            except Exception as e:
                raven.user_context(dict(system="email"))
                raven.user_context(dict(target=target, message=subject))
                raven.captureException(e)
    else:
        print(event)
        print("== Unknown Event ==")
# }}}


def send_message(arn, text, voice_id=DEFAULT_VOICE, text_type="text"):
    devices = []

    # Check whether `target` is Device or Group {{{
    is_group = False
    item = session.query(Device).filter(Device.arn.endswith(arn)).first()
    print("item:", item)
    if item is not None:
        # Found a device
        devices.append(item)
    else:
        # Check if `target` is Group
        group = session.query(Group).filter(Group.arn.endswith(arn)).first()
        if group is not None:
            print("group:", group, group.devices)
            is_group = True
            devices.extend(group.devices)
    # }}}

    # Generate mp3 file {{{
    response = polly.synthesize_speech(
        OutputFormat="mp3",
        Text=text,
        TextType=text_type,
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
