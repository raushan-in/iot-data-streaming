from dotenv import load_dotenv
import os

load_dotenv()

# Kafka Configuration
KAFKA_BROKER = os.getenv("IOT_KAFKA_BROKER")
KAFKA_TOPIC = os.getenv("IOT_KAFKA_TOPIC")
