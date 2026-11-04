import json

from kafka import KafkaConsumer
from kafka import KafkaProducer

from config import (
    KAFKA_BROKER,
    BUILDING_TOPIC,
    TEMP_TOPIC,
    HUMIDITY_TOPIC
)

consumer = KafkaConsumer(
    BUILDING_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda m:
        json.loads(m.decode("utf-8")),
    auto_offset_reset="earliest",
    group_id="alert_processor_group"
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v:
        json.dumps(v).encode("utf-8")
)

print("Processor started")

for msg in consumer:

    data = msg.value

    sensor_id = data["sensor_id"]
    temperature = data["temperature"]
    humidity = data["humidity"]
    timestamp = data["timestamp"]

    if temperature > 40:

        alert = {
            "sensor_id": sensor_id,
            "timestamp": timestamp,
            "temperature": temperature,
            "message": (
                f"Temperature exceeded: "
                f"{temperature} C"
            )
        }

        producer.send(
            TEMP_TOPIC,
            value=alert
        )

    if humidity > 80 or humidity < 20:

        alert = {
            "sensor_id": sensor_id,
            "timestamp": timestamp,
            "humidity": humidity,
            "message": (
                f"Humidity out of range: "
                f"{humidity}%"
            )
        }

        producer.send(
            HUMIDITY_TOPIC,
            value=alert
        )
        