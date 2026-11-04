import random
import time
from datetime import datetime

from kafka import KafkaProducer

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    BUILDING_SENSORS_TOPIC
)

from utils import serializer

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=serializer
)

sensor_id = random.randint(1000, 9999)

print(f"Sensor started. ID={sensor_id}")

while True:

    data = {
        "sensor_id": sensor_id,
        "timestamp": datetime.now().isoformat(),
        "temperature": round(random.uniform(25, 45), 2),
        "humidity": round(random.uniform(15, 85), 2)
    }

    producer.send(
        BUILDING_SENSORS_TOPIC,
        value=data
    )

    producer.flush()

    print(data)

    time.sleep(2)
    