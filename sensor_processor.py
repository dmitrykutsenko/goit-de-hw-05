from kafka import KafkaConsumer, KafkaProducer

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    BUILDING_SENSORS_TOPIC,
    TEMPERATURE_ALERTS_TOPIC,
    HUMIDITY_ALERTS_TOPIC
)

from utils import serializer, deserializer


consumer = KafkaConsumer(
    BUILDING_SENSORS_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    value_deserializer=deserializer,
    group_id="sensor-processor-group"
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=serializer
)

print("Processor started...")

for message in consumer:

    data = message.value

    sensor_id = data["sensor_id"]
    temperature = data["temperature"]
    humidity = data["humidity"]
    timestamp = data["timestamp"]

    if temperature > 40:

        alert = {
            "sensor_id": sensor_id,
            "timestamp": timestamp,
            "temperature": temperature,
            "message": f"Temperature exceeded threshold: {temperature}°C"
        }

        producer.send(
            TEMPERATURE_ALERTS_TOPIC,
            value=alert
        )

        print("Temperature alert sent")

    if humidity > 80 or humidity < 20:

        alert = {
            "sensor_id": sensor_id,
            "timestamp": timestamp,
            "humidity": humidity,
            "message": f"Humidity out of range: {humidity}%"
        }

        producer.send(
            HUMIDITY_ALERTS_TOPIC,
            value=alert
        )

        print("Humidity alert sent")

    producer.flush()
    