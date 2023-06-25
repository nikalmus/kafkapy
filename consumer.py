import sys
from confluent_kafka import Consumer, KafkaError
from topic import create_topic


def consume(consumer):
    while True:
        message = consumer.poll(30)
        if message is None:
            print("No message received...")
            continue

        if message.error():
            print(f"Consumer error: {message.error()}")
            if message.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f"Consumer error: {message.error()}")
                break
        print(f"Received message: {message.value().decode('utf-8')}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python consumer.py <topic> <bootstrap_servers>")
        sys.exit(1)

    topic = sys.argv[1]
    bootstrap_servers = sys.argv[2]
    create_topic(topic, bootstrap_servers)
    consumer = Consumer({"bootstrap.servers": bootstrap_servers, "group.id": 'group-toad', "auto.offset.reset": "earliest"})
    consumer.subscribe([topic])
    consume(consumer, topic)