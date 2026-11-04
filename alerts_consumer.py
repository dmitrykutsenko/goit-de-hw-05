from kafka import KafkaConsumer

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    TEMPERATURE_ALERTS_TOPIC,
    HUMIDITY_ALERTS_TOPIC
)

from utils import deserializer

consumer = KafkaConsumer(
    TEMPERATURE_ALERTS_TOPIC,
    HUMIDITY_ALERTS_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    value_deserializer=deserializer,
    group_id="alert-consumer-group"
)

print("Waiting for alerts...\n")

for message in consumer:

    topic = message.topic
    alert = message.value

    print("=" * 60)
    print(f"TOPIC: {topic}")
    print(alert)
    