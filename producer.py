import confluent_kafka

def produce():
    """Produces a message to the `hello_world` topic."""

    conf = {
        'bootstrap.servers': 'localhost:9092'
    }
    producer = confluent_kafka.Producer(conf)
    producer.produce("test", "fukabane")
    producer.flush()

if __name__ == "__main__":
    produce()