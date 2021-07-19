import datetime

from kafka_log_event.logic.client.mongo_client import db
from kafka_log_event.logic.client.kafka_client import Consumer
from kafka_log_event.utils.logger import logger


def insert_event(event):
    event_timestamp = event["event_timestamp"]
    user = event["user"]
    date = (
        datetime.datetime.utcfromtimestamp(int(event_timestamp))
        .astimezone()
        .replace(microsecond=0)
        .isoformat()
    )
    domain = user[user.find("@") + 1 :]
    data = {
        "event_timestamp": event_timestamp,
        "user": user,
        "mailbox": event["mailbox"],
        "event": event["event"],
        "uidvalidity": event["uidvalidity"],
        "uids": event["uids"],
        "date": date,
        "from": event["from"],
        "snippet": event.get("snippet", ""),
        "subject": event["subject"],
        "to": event["to"],
        "msgid": event["msgid"],
        "domain": domain,
    }
    if event["event"] == "MessageNew":
        logger.info(f"INSERT INTO MessageNew VALUES {data}")
        db.MessageNew.insert_one(data)
    else:
        logger.info(f"INSERT INTO MessageAppend VALUES {data}")
        db.MessageAppend.insert_one(data)


def poll_messages():
    consumer = Consumer()
    while True:
        msg = consumer.poll()
        if not msg:
            logger.info("poll timeout")
            continue
        for event in list(msg.values())[0]:
            offset = event.offset
            partition = event.partition
            topic = event.topic
            data = event.value
            event_type = data["event"]
            if event_type == "MessageNew" or (
                event_type == "MessageAppend" and data["mailbox"] == "Sent"
            ):
                logger.info(f"EVENT: {data}")
                insert_event(data)
                consumer.kafka_commit(topic, partition, offset)
                logger.info(
                    f"KAFKA COMMIT - TOPIC: {topic} - PARTITION: {partition} - OFFSET: {offset}"
                )
