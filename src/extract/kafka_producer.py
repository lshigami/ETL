from confluent_kafka import Producer
import json
from .config import get_kafka_config

class KafkaProducer:
    def __init__(self):
        config = get_kafka_config()
        self.producer = Producer({'bootstrap.servers': config['bootstrap_servers']})
        self.topic = config['topic']

    def delivery_report(self, err, msg):
        if err is not None:
            print(f"Message delivery failed: {err}")
        else:
            print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    def send_message(self, data):
        try:
            self.producer.produce(self.topic, json.dumps(data).encode('utf-8'), callback=self.delivery_report)
            self.producer.flush()
        except Exception as e:
            print(f"Error sending message to Kafka: {e}")