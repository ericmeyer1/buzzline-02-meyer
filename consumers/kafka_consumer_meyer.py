"""
Custom producer that emits extrusion sensor readings.
"""

import os
import random
import time
from dotenv import load_dotenv
from kafka import KafkaProducer
from utils.utils_logger import logger

#####################################
# Load Environment Variables
#####################################

load_dotenv()

# Initialize Kafka producer
def create_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=os.getenv("KAFKA_BROKER", "localhost:9092"),
            value_serializer=lambda v: str(v).encode("utf-8"),
        )
        logger.info("Kafka Producer created successfully.")
        return producer
    except Exception as e:
        logger.error(f"Failed to create Kafka producer: {e}")
        return None

#####################################
# Define a function to process a single message
# #####################################


def generate_sensor_message():
    temp = round(random.uniform(180, 230), 2)  # °C
    pressure = round(random.uniform(90, 120), 2)  # bar
    motor_speed = round(random.uniform(1400, 1600), 0)  # RPM

    if temp > 220:
        return f"ALERT: Extruder overheating! Temp={temp}°C"
    elif pressure > 115:
        return f"ALERT: High pressure detected! Pressure={pressure} bar"
    else:
        return f"Extrusion reading: Temp={temp}°C, Pressure={pressure} bar, Motor={motor_speed} RPM"


#####################################
# Define main function for this module
#####################################


def main():
    producer = create_producer()
    if not producer:
        return

    topic = os.getenv("KAFKA_TOPIC", "extrusion_data")

    try:
        logger.info("Starting Extrusion Sensor Producer...")
        while True:
            msg = generate_sensor_message()
            producer.send(topic, msg)
            logger.info(f"Sent: {msg}")
            time.sleep(2)
    except KeyboardInterrupt:
        logger.warning("Producer interrupted by user.")
    finally:
        producer.close()
        logger.info("Producer closed.")

#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
