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


def main() -> None:
    """
    Main entry point for the consumer.

    - Reads the Kafka topic name and consumer group ID from environment variables.
    - Creates a Kafka consumer using the `create_kafka_consumer` utility.
    - Processes messages from the Kafka topic.
    """
    logger.info("START consumer.")

    # fetch .env content
    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    logger.info(f"Consumer: Topic '{topic}' and group '{group_id}'...")

    # Create the Kafka consumer using the helpful utility function.
    consumer = create_kafka_consumer(topic, group_id)

     # Poll and process messages
    logger.info(f"Polling messages from topic '{topic}'...")
    try:
        for message in consumer:
            message_str = message.value
            logger.debug(f"Received message at offset {message.offset}: {message_str}")
            process_message(message_str)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error while consuming messages: {e}")
    finally:
        consumer.close()
        logger.info(f"Kafka consumer for topic '{topic}' closed.")

    logger.info(f"END consumer for topic '{topic}' and group '{group_id}'.")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
