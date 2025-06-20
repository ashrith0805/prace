from kafka import KafkaProducer
import json
import time

TOPIC_NAME = "test-topic"
KAFKA_BROKER = "kafka:9092"

while True:
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda m: json.dumps(m).encode('utf-8')
        )
        break
    except Exception as e:
        print("Kafka not ready, retrying in 5s...")
        time.sleep(5)

print("Producer started. Sending messages...")
for i in range(5):
    message = {"id": i, "message": f"Message {i}"}
    producer.send(TOPIC_NAME, message)
    print(f"Sent: {message}")
    time.sleep(1)

producer.flush()
