from confluent_kafka import Producer
from random import randint
from time import sleep


def get_producer(kafka_host="localhost:9092"):
    return Producer({
        'bootstrap.servers': kafka_host,
        'linger.ms': '1000',
        'compression.type': 'gzip'
    })


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message with key {} delivered to {} [{}]'.format('', msg.topic(), msg.partition()))


def produce(producer, messages):
    for data in messages:
        # Trigger any available delivery report callbacks from previous produce() calls
        producer.poll(0)

        # Asynchronously produce a message, the delivery report callback
        # will be triggered from poll() above, or flush() below, when the message has
        # been successfully delivered or failed permanently.
        print("Attempting to produce: {}".format(data))
        producer.produce(
            'quote-feedback',
            data['message'].encode('utf-8'),
            key=bytes(data['key']),
            callback=delivery_report
        )

    # Wait for any outstanding messages to be delivered and delivery report
    # callbacks to be triggered.
    producer.flush()


def produce_many(producer, count, sleep_time=0.01, message="Order accepted."):
    producer.poll(0)

    print("Attempting to produce {} messages".format(count))
    i = 1
    while i < count:
        producer.produce(
            'quote-feedback',
            message.encode('utf-8'),
            key=bytes(randint(1, 1500)),
            callback=delivery_report
        )
        sleep(sleep_time)
        i += 1

    producer.flush()


def main():
    producer = get_producer()
    messages = [
        {'key': 1, 'message': "Order 28554 accepted"},
        {'key': 2, 'message': "Order 28587 accepted"},
        {'key': 3, 'message': "Order 285874 accepted"}
    ]
    produce(producer, messages)


if __name__ == '__main__':
    main()
