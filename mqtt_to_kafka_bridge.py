'''
Responsible for generating RFID data and sending it to Kafka
'''
import json
import random
import time
from datetime import datetime
from confluent_kafka import Producer
from config import KAFKA_BROKER, KAFKA_TOPIC

# Kafka Producer setup
kafka_producer = Producer({"bootstrap.servers": KAFKA_BROKER})


# Kafka delivery confirmation callback
def kafka_delivery_callback(err, msg):
    if err:
        print(f"Failed to deliver message to Kafka: {err}")
    else:
        print(
            f"Message delivered to Kafka topic {msg.topic()} [partition {msg.partition()}]"
        )


# Function to generate random JSON messages
def generate_random_message():
    uid = random.randint(1000, 9999)  # Random UID for the device
    zone_code = f"Zone-{random.randint(1, 5)}"  # Random zone code
    location_ping = random.choice(["entry", "exit", "center"])  # Random location
    timestamp = datetime.now().isoformat()  # Current timestamp
    purchase_code = f"PC-{random.randint(10000, 99999)}"  # Random purchase code
    return {
        "uid": uid,
        "zone_code": zone_code,
        "location_ping": location_ping,
        "timestamp": timestamp,
        "purchase_code": purchase_code,
    }


def main():
    print("Starting MQTT to Kafka message simulator...")
    start_time = time.time()
    duration = 3600  # Run for an hour (3600 seconds)

    while time.time() - start_time < duration:
        try:
            # Generate a random message
            message = generate_random_message()

            # Produce message to Kafka
            kafka_producer.produce(
                KAFKA_TOPIC, value=json.dumps(message), callback=kafka_delivery_callback
            )

            # kafka_producer.flush()  # Ensure the message is sent immediately
            print(f"Sent message: {message}")

            # Simulate real-time data streaming with a delay
            time.sleep(random.uniform(0.5, 2.0))  # Random delay between messages
        except Exception as e:
            print(f"Error during message generation or Kafka delivery: {e}")

    print("Finished MQTT to Kafka simulation after 1 hour.")


if __name__ == "__main__":
    main()
