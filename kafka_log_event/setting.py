from environs import Env

env = Env()
env.read_env()


class Kafka:
    KAFKA_BROKER = env.list("KAFKA_BROKER")
    KAFKA_TOPIC = env.str("KAFKA_TOPIC")
    KAFKA_GROUP_ID = env.str("KAFKA_GROUP_ID")
    KAFKA_ENABLE_AUTO_COMMIT = env.bool("KAFKA_ENABLE_AUTO_COMMIT", True)
    KAFKA_AUTO_OFFSET_RESET = env.str("KAFKA_AUTO_OFFSET_RESET")
    KAFKA_MAX_POLL_RECORDS = env.int("KAFKA_MAX_POLL_RECORDS")


class KafkaAuth:
    SASL_PLAIN_USERNAME = env.str("SASL_PLAIN_USERNAME")
    SASL_PLAIN_PASSWORD = env.str("SASL_PLAIN_PASSWORD")
    SECURITY_PROTOCOL = env.str("SECURITY_PROTOCOL")
    SASL_MECHANISM = env.str("SASL_MECHANISM")


class MongoDb:
    MONGO_URI = env.str("MONGO_URI")
    MONGO_DB = env.str("MONGO_DB")
