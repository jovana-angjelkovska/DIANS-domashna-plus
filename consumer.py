from kafka import KafkaConsumer
import json

# Kafka configuration
BROKER = 'localhost:9092'
TOPIC = 'stock-data'

# Initialize Kafka consumer
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BROKER,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print(f"Listening to topic {TOPIC}...")

# Consume messages from the topic
for message in consumer:
    print(f"Received message from topic {TOPIC}: {message.value}")
