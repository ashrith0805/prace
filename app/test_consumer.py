import unittest
from unittest.mock import patch, MagicMock
from kafka import KafkaConsumer

class TestKafkaConsumer(unittest.TestCase):

    @patch('kafka.KafkaConsumer')
    def test_consumer_initialization(self, mock_consumer):
        topic = "test-topic"
        broker = "localhost:9092"
        mock_instance = MagicMock()
        mock_consumer.return_value = mock_instance

        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=[broker],
            value_deserializer=lambda m: m.decode("utf-8")
        )

        mock_consumer.assert_called_with(
            topic,
            bootstrap_servers=[broker],
            value_deserializer=unittest.mock.ANY
        )

if __name__ == '__main__':
    unittest.main()
