import confluent_kafka

def consume():
    """Consumes messages from the `hello_world` topic."""

    conf = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'consumer-test-1',
        'auto.offset.reset': 'earliest'
    }
    consumer = confluent_kafka.Consumer(conf)
    consumer.subscribe(["test"])

    while True:
        message = consumer.poll(100)
        if message is None:
            continue

        if message.error():
            print(message.error())
            continue

        print(message.value())

if __name__ == "__main__":
    consume()