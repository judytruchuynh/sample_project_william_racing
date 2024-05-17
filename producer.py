import json
import time
import random
from kafka import KafkaProducer

# Define the Kafka topic
TOPIC_NAME = 'f1_telemetry'

# Create a Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_telemetry_data():
    # Generate random telemetry data for an F1 car
    data = {
        'car_id': random.randint(1, 20),  # Car IDs typically between 1 and 20
        'speed': random.uniform(100, 350),  # Speed in km/h
        'throttle': random.uniform(0, 100),  # Throttle percentage
        'brake': random.uniform(0, 100),  # Brake percentage
        'gear': random.randint(1, 8),  # Current gear
        'rpm': random.randint(8000, 15000),  # Engine RPM
        'lap_time': random.uniform(60, 120),  # Lap time in seconds
        'timestamp': time.time()
    }
    return data

while True:
    telemetry_data = generate_telemetry_data()
    print(f"Sending: {telemetry_data}")
    producer.send(TOPIC_NAME, telemetry_data)
    time.sleep(1)  # Send data every second
