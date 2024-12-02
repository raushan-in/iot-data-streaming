# File: mqtt_to_kafka_bridge.py

import paho.mqtt.client as mqtt
from confluent_kafka import Producer
import json

# MQTT Configuration
MQTT_BROKER = "broker.hivemq.com"
MQTT_PORT = 1883
MQTT_TOPIC = "test/magic_band/iot"
MQTT_CLIENT_ID = "mqtt_to_kafka_bridge"

# Kafka Configuration
KAFKA_BROKER = "localhost:9092"  # Replace with your Kafka broker address
KAFKA_TOPIC = "iot_data_stream"

# Kafka Producer setup
kafka_producer = Producer({'bootstrap.servers': KAFKA_BROKER})

# Callback to confirm message delivery to Kafka
def kafka_delivery_callback(err, msg):
    if err:
        print(f"Failed to deliver message to Kafka: {err}")
    else:
        print(f"Message delivered to Kafka topic {msg.topic()} [partition {msg.partition()}]")

# Callback for MQTT connection
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT broker!")
        client.subscribe(MQTT_TOPIC)  # Subscribe to the MQTT topic
    else:
        print(f"Failed to connect to MQTT broker with code {rc}")

# Callback for MQTT message
def on_message(client, userdata, msg):
    try:
        # Decode the MQTT message
        mqtt_message = msg.payload.decode()
        print(f"Received MQTT message: {mqtt_message} on topic: {msg.topic}")

        # Send the message to Kafka
        kafka_producer.produce(
            KAFKA_TOPIC,
            value=json.dumps({"mqtt_topic": msg.topic, "message": mqtt_message}),
            callback=kafka_delivery_callback
        )
        kafka_producer.flush()
    except Exception as e:
        print(f"Error processing message: {e}")

def main():
    # MQTT Client setup
    mqtt_client = mqtt.Client(MQTT_CLIENT_ID)
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message

    # Connect to MQTT broker
    mqtt_client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)

    # Start MQTT loop
    mqtt_client.loop_forever()

if __name__ == "__main__":
    main()
