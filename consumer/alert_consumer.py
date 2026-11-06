import json

from kafka import KafkaConsumer

from config import (
    KAFKA_BROKER,
    TEMP_TOPIC,
    HUMIDITY_TOPIC
)

consumer = KafkaConsumer(
    TEMP_TOPIC,
    HUMIDITY_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="earliest",
    value_deserializer=lambda m:
        json.loads(m.decode("utf-8")),
    group_id="alert_consumer_group_v2"
)

print("Alert consumer started")
print(f"Listening topics: {TEMP_TOPIC}, {HUMIDITY_TOPIC}")

for message in consumer:

    alert = message.value

    print("\n==============================")
    print(f"TOPIC: {message.topic}")
    print(alert)
    print("==============================")