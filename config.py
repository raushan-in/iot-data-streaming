from dotenv import load_dotenv
import os

load_dotenv()

# Kafka Configuration
KAFKA_BROKER = os.getenv("IOT_KAFKA_BROKER")
KAFKA_TOPIC = os.getenv("IOT_KAFKA_TOPIC")

influx_bucket = os.getenv("INFLUX_BUCKET")
influx_org = os.getenv("INFLUX_ORG")
influx_token = os.getenv("INFLUX_TOKEN")
influx_url = os.getenv("INFLUX_URL")
