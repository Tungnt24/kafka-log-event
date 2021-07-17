import datetime
import json

from kafka import KafkaConsumer
from kafka_log_event.logic.client.mongo_client import db
from kafka_log_event.utils.logger import logger


def get_consumer():
    consumer = consumer = KafkaConsumer(
        "",
        group_id="",
        bootstrap_servers=[],
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")))
    return consumer


def insert_event(event):
    event_timestamp = event['event_timestamp']
    date = datetime.datetime.utcfromtimestamp(int(event_timestamp)).astimezone().replace(microsecond=0).isoformat()
    data = {
        "event_timestamp": event_timestamp,
        "user": event['user'],
        "mailbox": event['mailbox'],
        "event": event['event'],
        "uidvalidity": event['uidvalidity'],
        "uids": event['uids'],
        "date": date,
        "from": event['from'],
        "snippet": event.get('snippet', ""),
        "subject": event['subject'],
        "to": event['to'],
        "msgid": event['msgid']
    }
    if event['event'] == "MessageNew":
        db.MessageNew.insert_one(data)
    else:
        db.MessageAppend.insert_one(data)


def poll_messages():
    consumer = get_consumer()
    while True:
        msg = consumer.poll(1000)
        if not msg:
            logger.info("poll timeout")
            continue
        for event in list(msg.values())[0]:
            data = event.value
            if data['event'] not in ["MessageNew", "MessageAppend"]:
                continue
            if data['event'] == "MessageAppend" and data['mailbox'] != "Sent":
                continue
            logger.info(f"EVENT {data}")
            insert_event(data)
