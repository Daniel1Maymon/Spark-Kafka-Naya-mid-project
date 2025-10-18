from kafka import KafkaProducer
import json
from datetime import datetime, timezone
import uuid
import time

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Create a single alert message
event = {
    "event_id": str(uuid.uuid4()),
    "event_time": datetime.now(timezone.utc).isoformat(),
    "car_id": 5417867,
    "driver_id": 702937568,
    "brand_name": "Kia",
    "model_name": "Rio",
    "color_name": "Blue",
    "speed": 150.5,
    "rpm": 3200,
    "gear": 4,
    "expected_gear": 6
}

# Send message to Kafka topic 'alert-data'
producer.send("alert-data", value=event)
producer.flush()

print(f"Sent message at {event['event_time']} to topic 'alert-data'")
