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
        json.loads(m.decode("utf-8"))
)

print("Alert consumer started")

for message in consumer:

    alert = message.value

    print(
        f"[{message.topic}] "
        f"{alert}"
    )
    