import sys
from confluent_kafka.admin import AdminClient, NewTopic

def create_topic(topic, bootstrap_servers, num_partitions):
    try:
        admin_client = AdminClient({"bootstrap.servers": bootstrap_servers})
        print("Brokers are valid and accessible.")
    except Exception as e:
        print(f"Error connecting to brokers: {e}")
        sys.exit(1)

    # Check if the topic already exists
    topic_metadata = admin_client.list_topics(timeout=10).topics
    if topic in topic_metadata:
        print(f"Topic '{topic}' already exists. Skipping creation.")
        return
    topic_config = {
        "cleanup.policy": "delete",
        "retention.ms": "86400000"
        # Add any additional topic configuration parameters as needed
    }
    new_topic = NewTopic(topic, num_partitions, config=topic_config)
    futures = admin_client.create_topics([new_topic])
    for topic, future in futures.items():
        try:
            future.result()  # Wait for the topic creation to complete
            print(f"Topic '{topic}' created successfully.")
        except Exception as e:
            print(f"Failed to create topic '{topic}': {e}")