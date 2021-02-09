from confluent_kafka.admin import AdminClient, NewTopic
import subprocess

def get_client(kafka_host="localhost:9092"):
    """ Get Kafka admin client for the given host and port
    :param kafka_host: Kafka host:port
    :return: Kafka admin client
    """
    return AdminClient({'bootstrap.servers': kafka_host})


def print_all_topics(client):
    """ Print all topics for a given admin client
    :param client: Kafka admin client
    :return: void
    """
    topics = client.list_topics().topics
    print("Topics in the cluster:")
    if bool(topics) == 1:
        for topic in topics:
            print(topic)
    else:
        print('No topics found')
    print('\n')


def create_topics(client):
    """ Create topics in the given admin client
    :param client: Kafka admin client
    :return:  void
    """
    print('Creating new topics')
    new_topics = [NewTopic(topic, num_partitions=2, replication_factor=1)
                  for topic in ["quote-feedback"]]

    # Call create_topics to asynchronously create topics. A dict
    # of <topic,future> is returned.
    fs = client.create_topics(new_topics)

    # Wait for each operation to finish.
    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            print("Topic {} created".format(topic))
        except Exception as e:
            print("Failed to create topic {}: {}".format(topic, e))
    print('\n')


def describe_consumer_group(consumer_group):
    bash_cmd = [
        "docker-compose",
        "exec",
        "kafka",
        "/bin/kafka-consumer-groups",
        "--bootstrap-server",
        "localhost:9092",
        "--describe",
        "--group",
        consumer_group
    ]
    process = subprocess.Popen(bash_cmd, stdout=subprocess.PIPE)
    output, error = process.communicate()
    print(str(output).replace("\\r\\n", "\n"))


def main():
    admin = get_client()
    print_all_topics(admin)
    create_topics(admin)
    print_all_topics(admin)


if __name__ == '__main__':
    main()
