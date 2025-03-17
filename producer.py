from kafka import KafkaProducer
import json
import time

# Kafka configuration
BROKER = 'localhost:9092'
TOPIC = 'stock-data'

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Example data to send to Kafka
data = [
    {"symbol": "AAPL", "timestamp": "2025-03-02T23:00:00Z", "open": 150, "high": 155, "low": 149, "close": 154, "volume": 1000},
    {"symbol": "GOOG", "timestamp": "2025-03-02T23:01:00Z", "open": 2800, "high": 2820, "low": 2790, "close": 2810, "volume": 500}
]

# Send data to Kafka topic
for record in data:
    producer.send(TOPIC, record)
    print(f"Sent record to topic {TOPIC}: {record}")
    time.sleep(1)  # Simulate real-time data

producer.close()
