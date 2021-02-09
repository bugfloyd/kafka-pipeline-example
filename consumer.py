from confluent_kafka import Consumer


def get_consumer(kafka_host="localhost:9092"):
    return Consumer({
        'bootstrap.servers': kafka_host,
        'group.id': 'consumerGroup',
        'auto.offset.reset': 'earliest',
        'max.poll.interval.ms': '10000',
    })


def subscribe(consumer, topic):
    consumer.subscribe([topic])


def poll_loop(consumer):
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            print("No message to show!")
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue

        print("Message {} at offset {} of partition {}".format(msg.value().decode('utf-8'), msg.offset(), msg.partition()))

    consumer.close()


def main():
    consumer = get_consumer()
    subscribe(consumer, "quote-feedback")
    poll_loop(consumer)


if __name__ == '__main__':
    main()
