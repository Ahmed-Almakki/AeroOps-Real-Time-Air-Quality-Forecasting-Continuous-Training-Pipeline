import pytest
from unittest.mock import patch, MagicMock
from confluent_kafka import KafkaException

from src.input_data.kafka_input import KafkaInput


class Test_KafkaInput:

    @patch('src.input_data.kafka_input.Consumer')
    def test_get_single_message(self, mock_consumer):
        mock_consumer_instance = mock_consumer.return_value

        mock_msg = MagicMock()
        # because when the funcion do msg.error it find there is no error
        mock_msg.error.return_value = None
        mock_msg.value.return_value = b'{"status": "successfully delivered message"}'
        mock_consumer_instance.poll.return_value = mock_msg

        kafka_input = KafkaInput(topic="test_topic")
        result = kafka_input.get_single_message()

        assert type(result) == dict
        assert result["status"] == "successfully delivered message"

        mock_consumer_instance.commit.assert_called_once_with(mock_msg)

    @patch('src.input_data.kafka_input.Consumer')
    def test_get_single_message_returns_none(self, mock_consumer):
        """Test the scenario where poll() times out and returns None."""
        mock_consumer_instance = mock_consumer.return_value

        mock_consumer_instance.poll.return_value = None

        kafka_input = KafkaInput(topic="test_topic")
        result = kafka_input.get_single_message()

        assert result is None

    @patch('src.input_data.kafka_input.Consumer')
    def test_get_single_message_raises_kafka_exception(self, mock_consumer):
        """Test the scenario where poll() returns a message with a fatal error."""
        mock_consumer_instance = mock_consumer.return_value

        mock_msg = MagicMock()

        mock_error = MagicMock()

        mock_error.code.return_value = "SOME_FATAL_ERROR_CODE"

        mock_msg.error.return_value = mock_error

        mock_consumer_instance.poll.return_value = mock_msg

        kafka_input = KafkaInput(topic="test_topic")

        with pytest.raises(KafkaException):
            kafka_input.get_single_message()
