from testcontainers.kafka import KafkaContainer
from confluent_kafka import Producer
import pytest
from src.input_data.kafka_input import KafkaInput



def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for record {msg.key()}: {err}")
    else:
        print(f"Record {msg.key()} successfully produced to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")


@pytest.fixture(scope="session")
def kafka():
    with KafkaContainer() as kafka:
        connection = kafka.get_bootstrap_server()
        yield connection


def test_get_single_message(kafka):
    topics = "integration_test_topic"

    producer_configuration = {
        'bootstrap.servers': kafka,
        'client.id': 'test_producer'
    }

    consumer_configuration = {
        'bootstrap.servers': kafka,
        'group.id': 'my_group',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
    }


    # Produce a test message to the Kafka topic
    with Producer(producer_configuration) as producer:
        producer.produce(
            topic=topics,
            key="test_key",
            value='{"test_key": "test_value"}',
            callback=delivery_report
        )
        producer.flush()

    kafka_instance = KafkaInput(topic=topics, conf=consumer_configuration)

    # because consumer take moment before it jooin consumer group and get partition assignment
    # we will try to get message for 10 times
    msg = None
    for _ in range(10):
        msg = kafka_instance.get_single_message()
        if msg is not None:
            break
    assert msg is not None
    assert msg == {"test_key": "test_value"}
