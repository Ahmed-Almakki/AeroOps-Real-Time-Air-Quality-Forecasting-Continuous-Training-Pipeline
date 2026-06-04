from confluent_kafka import Consumer, KafkaException, KafkaError
import json
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

conf = {
    'bootstrap.servers': 'localhost:9092',
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

    def consume(self):
        """Continuously poll for messages from the Kafka topic, process them, and commit offsets."""
        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    logging.error(f"Error occurred while consuming message: {msg.error()}")
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logging.info(f"End of partition reached {msg.topic()} [{msg.partition()}]")
                    else:
                        raise KafkaException(msg.error())
                else:
                    data = json.loads(msg.value().decode('utf-8'))
                    logging.info(f"Received message: {data}")
                    self.consumer.commit(msg)
                    logging.info("Offset committed successfully.")
                    return data
        except KeyboardInterrupt:
            logging.info("Kafka consumer interrupted by user.")
            pass
        finally:
            self.consumer.close()