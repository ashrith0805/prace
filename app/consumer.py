from kafka import KafkaConsumer
import json

TOPIC_NAME = "test-topic"
KAFKA_BROKER = "kafka:9092"

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=[KAFKA_BROKER],
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("Consumer started. Listening for messages...")

for message in consumer:
    print(f"Received: {message.value}")
