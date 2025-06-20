import logging
from kafka import KafkaConsumer
import json
import time

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("consumer.log"),   # Log to file
        logging.StreamHandler()               # Log to console
    ]
)

TOPIC_NAME = "test-topic"
KAFKA_BROKER = "kafka:9092"

try:
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=[KAFKA_BROKER],
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    logging.info("KafkaConsumer initialized successfully.")
except Exception as e:
    logging.error("Failed to initialize KafkaConsumer.", exc_info=True)
    raise

def run_consumer():
    logging.info("Consumer started. Listening for messages...")
    try:
        for message in consumer:
            logging.info(f"Received message: {message.value}")
    except Exception as e:
        logging.error("Error occurred while consuming messages.", exc_info=True)

if __name__ == "__main__":
    run_consumer()
