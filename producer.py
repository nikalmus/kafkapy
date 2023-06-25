import sys
from confluent_kafka import Producer
from topic import create_topic

def produce(producer, topic):
    print(f"{producer} produce_messages for {topic}...")
    for i in range(10):
        message = f"Message {i}"
        producer.produce(topic, message)
        print(f"Produced message: {message}")
    producer.flush()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python producer.py <topic> <bootstrap_servers>")
        sys.exit(1)

    topic = sys.argv[1]
    bootstrap_servers = sys.argv[2]
    create_topic(topic, bootstrap_servers)
    producer = Producer({"bootstrap.servers": bootstrap_servers})
    produce(producer, topic)