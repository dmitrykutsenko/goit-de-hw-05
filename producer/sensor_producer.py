import json
import random
import time
from datetime import datetime

from kafka import KafkaProducer


from config import (
    KAFKA_BROKER,
    BUILDING_TOPIC
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

sensor_id = random.randint(1000, 9999)

print(f"Sensor started: {sensor_id}")
print(f"Sending data to: {BUILDING_TOPIC}")

while True:

    data = {
        "sensor_id": sensor_id,
        "timestamp": datetime.utcnow().isoformat(),
        "temperature": round(
            random.uniform(25, 45),
            2
        ),
        "humidity": round(
            random.uniform(15, 85),
            2
        )
    }

    producer.send(
        BUILDING_TOPIC,
        value=data
    )

    producer.flush()

    print(data)

    time.sleep(5)
