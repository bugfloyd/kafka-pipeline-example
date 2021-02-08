from confluent_kafka import Producer


def get_producer(kafka_host="localhost:9092"):
    return Producer({
        'bootstrap.servers': kafka_host,
    })


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


def produce_messages(producer, messages):
    for data in messages:
        # Trigger any available delivery report callbacks from previous produce() calls
        producer.poll(0)

        # Asynchronously produce a message, the delivery report callback
        # will be triggered from poll() above, or flush() below, when the message has
        # been successfully delivered or failed permanently.
        print("Attempting to produce: {}".format(data))
        producer.produce('quote-feedback', data.encode('utf-8'), callback=delivery_report)

    # Wait for any outstanding messages to be delivered and delivery report
    # callbacks to be triggered.
    producer.flush()


def main():
    producer = get_producer()
    messages = ["Order 28554 accepted", "Order 28587 accepted", "Order 285874 accepted"]
    produce_messages(producer, messages)


if __name__ == '__main__':
    main()
