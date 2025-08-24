from confluent_kafka import Producer, Consumer, KafkaError
import json
import logging


class KafkaProducerClient:
    def __init__(self, bootstrap_servers: str, client_id: str):
        self.bootstrap_servers = bootstrap_servers
        self.client_id = client_id
        self.producer: Producer | None = None

    def connect(self):
        config = {
            "bootstrap.servers": self.bootstrap_servers,
            "client.id": self.client_id,
            "linger.ms": 10,  # small batching delay
        }
        self.producer = Producer(config)

    def delivery_report(self, err, msg):
        """Callback for delivery reports."""
        if err is not None:
            logging.error(f"Delivery failed: {err}")
        else:
            logging.info(
                f"Message delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}"
            )

    def send_message(self, topic: str, key: str | None, value: dict):
        if self.producer is None:
            raise AssertionError("Producer not connected. Call connect() first.")

        payload = json.dumps(value).encode("utf-8")
        self.producer.produce(
            topic=topic,
            key=key.encode("utf-8") if key else None,
            value=payload,
            callback=self.delivery_report,
        )

    def flush(self):
        """Ensure all buffered messages are sent."""
        if self.producer:
            self.producer.flush()

    def close(self):
        """Flush and close producer."""
        self.flush()
        self.producer = None


class KafkaConsumerClient:
    def __init__(
        self,
        bootstrap_servers: str,
        group_id: str,
        topics: list[str],
        auto_offset_reset: str = "earliest",
    ):
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.topics = topics
        self.auto_offset_reset = auto_offset_reset
        self.consumer: Consumer | None = None

    def connect(self):
        """Initialize the Kafka consumer and subscribe to topics."""
        config = {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": self.group_id,
            "auto.offset.reset": self.auto_offset_reset,
        }
        self.consumer = Consumer(config)
        self.consumer.subscribe(self.topics)

    def consume_messages(self, callback, timeout: float = 1.0):
        if self.consumer is None:
            raise AssertionError("Consumer not connected. Call connect() first.")

        while True:
            msg = self.consumer.poll(timeout)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue  # end of partition
                else:
                    logging.error(f"Consumer error: {msg.error()}")
                    continue

            key = msg.key().decode("utf-8") if msg.key() else None
            value = json.loads(msg.value().decode("utf-8"))
            callback(key, value)

    def commit_offset(self):
        """Commit the current offsets."""
        if self.consumer:
            self.consumer.commit()

    def close(self):
        """Close the consumer gracefully."""
        if self.consumer:
            self.consumer.close()
            self.consumer = None
