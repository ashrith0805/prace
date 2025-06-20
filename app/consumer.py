from kafka import KafkaConsumer
import json
import time

TOPIC_NAME = "test-topic"
KAFKA_BROKER = "kafka:9092"

while True:
    try:
        consumer = KafkaConsumer(
            TOPIC_NAME,
            bootstrap_servers=[KAFKA_BROKER],
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        print("Connected to Kafka.")
        break
    except Exception as e:
        print(f"Kafka not ready: {e}. Retrying in 5s...")
        time.sleep(5)

print("Consumer started. Listening for messages...")

for message in consumer:
    print(f"Received: {message.value}")
