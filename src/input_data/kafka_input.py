from confluent_kafka import Consumer, KafkaException, KafkaError
from dotenv import load_dotenv
import json
import logging
import os


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
load_dotenv()
conf = {
    'bootstrap.servers': os.getenv("BOOTSTRAP_SERVER"),
    'group.id': 'my_group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
}

class KafkaInput:
    """
    KafkaInput class to consume messages from a Kafka topic.
    It initializes a Kafka consumer and subscribes to the specified topic.
    The consume method continuously polls for messages, processes them, and commits the offsets.
    """
    def __init__(self, topic):
        self.consumer = Consumer(conf)
        self.consumer.subscribe([topic])

    def get_single_message(self):
        """Get a single message from the Kafka topic."""
        
        msg = self.consumer.poll(1.0)
        if msg is None:
            return None
        if msg.error():
            logging.error(f"Error occurred while consuming message: {msg.error()}")
            if msg.error().code() == KafkaError._PARTITION_EOF:
                logging.info(f"End of partition reached {msg.topic()} [{msg.partition()}]")
            else:
                logging.error(f"Error occurred while consuming message: {msg.error()}")
                raise KafkaException(msg.error())
        else:
            data = json.loads(msg.value().decode('utf-8'))
            logging.info(f"Received message successfully")
            self.consumer.commit(msg)
            logging.info("Offset committed successfully.")
            return data
        
    def close(self):
        """Close the Kafka consumer."""
        self.consumer.close()