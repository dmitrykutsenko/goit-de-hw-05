from kafka.admin import KafkaAdminClient, NewTopic

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    BUILDING_SENSORS_TOPIC,
    TEMPERATURE_ALERTS_TOPIC,
    HUMIDITY_ALERTS_TOPIC
)

admin_client = KafkaAdminClient(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    client_id="iot_admin"
)

topics = [
    NewTopic(
        name=BUILDING_SENSORS_TOPIC,
        num_partitions=1,
        replication_factor=1
    ),
    NewTopic(
        name=TEMPERATURE_ALERTS_TOPIC,
        num_partitions=1,
        replication_factor=1
    ),
    NewTopic(
        name=HUMIDITY_ALERTS_TOPIC,
        num_partitions=1,
        replication_factor=1
    ),
]

try:
    admin_client.create_topics(
        new_topics=topics,
        validate_only=False
    )
    print("Topics successfully created.")
except Exception as e:
    print(f"Topics may already exist: {e}")

admin_client.close()
