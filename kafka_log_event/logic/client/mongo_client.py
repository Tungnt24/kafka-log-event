from kafka_log_event.setting import MongoDb

import pymongo

conn = pymongo.MongoClient(MongoDb.MONGO_URI)
db = conn[MongoDb.MONGO_DB]
