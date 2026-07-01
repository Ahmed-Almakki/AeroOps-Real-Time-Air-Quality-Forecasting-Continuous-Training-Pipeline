import os
import json
import logging

from dotenv import load_dotenv
from confluent_kafka import Consumer, KafkaError, KafkaException

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
load_dotenv()


class KafkaInput:
    """
    KafkaInput class to consume messages from a Kafka topic.
    It initializes a Kafka consumer and subscribes to the specified topic.
    The consume method continuously polls for messages, processes them, and commits the offsets.
    """

    def __init__(self, topic, conf):
        self.consumer = Consumer(conf)
        self.consumer.subscribe([topic])

    # pylint: disable=inconsistent-return-statements
    def get_single_message(self):
        """Get a single message from the Kafka topic."""

        msg = self.consumer.poll(1.0)
        if msg is None:
            return None
        if msg.error():
            logging.error("Error occurred while consuming message: %s", msg.error())
            # pylint: disable=protected-access
            if msg.error().code() == KafkaError._PARTITION_EOF:
                logging.info("End of partition reached %s [%s]", msg.topic(), msg.partition())
            else:
                logging.error("Error occurred while consuming message: %s", msg.error())
                raise KafkaException(msg.error())
        else:
            data = json.loads(msg.value().decode('utf-8'))
            logging.info("Received message successfully")
            self.consumer.commit(msg)
            logging.info("Offset committed successfully.")
            return data

    def close(self):
        """Close the Kafka consumer."""
        self.consumer.close()
