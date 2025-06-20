import unittest
from unittest.mock import patch, MagicMock

class TestKafkaConsumer(unittest.TestCase):

    @patch('consumer.KafkaConsumer')  # 👈 PATCH WHERE IT'S USED
    def test_consumer_initialization(self, mock_consumer):
        topic = "test-topic"
        broker = "localhost:9092"
        mock_instance = MagicMock()
        mock_consumer.return_value = mock_instance

        from app import consumer  # Import after mocking

        # Now, creating the consumer inside app.consumer doesn't cause a real connection
        self.assertTrue(mock_consumer.called)
        mock_consumer.assert_called_with(
            topic,
            bootstrap_servers=[broker],
            value_deserializer=unittest.mock.ANY
        )

if __name__ == '__main__':
    unittest.main()
