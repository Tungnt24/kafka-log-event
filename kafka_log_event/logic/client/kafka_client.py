import json
import ssl

from kafka import KafkaConsumer
from kafka.structs import TopicPartition, OffsetAndMetadata

from kafka_log_event.setting import Kafka, KafkaAuth

context = ssl.create_default_context()
context.options &= ssl.OP_NO_TLSv1
context.options &= ssl.OP_NO_TLSv1_1


class Consumer:
    def __init__(self) -> None:
        self.consumer = KafkaConsumer(
            group_id=Kafka.KAFKA_GROUP_ID,
            bootstrap_servers=Kafka.KAFKA_BROKER,
            sasl_plain_username=KafkaAuth.SASL_PLAIN_USERNAME,
            sasl_plain_password=KafkaAuth.SASL_PLAIN_PASSWORD,
            security_protocol=KafkaAuth.SECURITY_PROTOCOL,
            sasl_mechanism=KafkaAuth.SASL_MECHANISM,
            ssl_context=context,
            auto_offset_reset=Kafka.KAFKA_AUTO_OFFSET_RESET,
            value_deserializer=lambda x: json.loads(
                x.decode("utf-8", "ignore")
            ),
            enable_auto_commit=Kafka.KAFKA_ENABLE_AUTO_COMMIT,
            max_poll_records=Kafka.KAFKA_MAX_POLL_RECORDS,
        )
        self.consumer.subscribe(pattern=Kafka.KAFKA_TOPIC)

    def poll(self):
        msg = self.consumer.poll(1000)
        return msg

    def kafka_commit(self, topic, partition, offset):
        tp = TopicPartition(topic, partition)
        self.consumer.commit({tp: OffsetAndMetadata(offset + 1, None)})
