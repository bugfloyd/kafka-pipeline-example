#!/usr/bin/env python
import argparse
from six.moves import input
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from message_objects import UserQuoteKey, UserQuoteValue


def user_quote_key_to_dict(user_quote_key, ctx):
    """
    Returns a dict representation of a User_Quote_Key instance for serialization.

    Args:
        user_quote_key (User_Quote_Key): User_Quote instance.

        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.

    Returns:
        dict: Dict populated with user quote key be serialized.

    """
    return dict(user_id=user_quote_key.user_id)


def user_quote_value_to_dict(user_quote_value, ctx):
    """
    Returns a dict representation of a User_Quote_value instance for serialization.

    Args:
        user_quote_value (User_Quote_value): User_Quote_value instance.

        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.

    Returns:
        dict: Dict populated with user quote attributes to be serialized.

    """
    return dict(product_id=user_quote_value.product_id,
                quoted_price=user_quote_value.quoted_price,
                quoted_quantity=user_quote_value.quoted_quantity,
                user_note=user_quote_value.user_note)


def delivery_report(err, msg):
    """
    Reports the failure or success of a message delivery.

    Args:
        err (KafkaError): The error that occurred on None on success.

        msg (Message): The message that was produced or failed.

    Note:
        In the delivery report callback the Message.key() and Message.value()
        will be the binary format as encoded by any configured Serializers and
        not the same object that was passed to produce().
        If you wish to pass the original object(s) for key and value to delivery
        report callback we recommend a bound callback or lambda where you pass
        the objects along.

    """
    if err is not None:
        print("Delivery failed for User_Quote record {}: {}".format(msg.key(), err))
        return
    print('User_Quote record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


def main(args):
    topic = args.topic

    key_schema_str = open('schema/KeySchema.avsc', "r").read()
    value_schema_str = open('schema/ValueSchema.avsc', "r").read()
    schema_registry_conf = {'url': args.schema_registry}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    avro_key_serializer = AvroSerializer(key_schema_str, schema_registry_client, user_quote_key_to_dict)
    avro_value_serializer = AvroSerializer(value_schema_str, schema_registry_client, user_quote_value_to_dict)

    producer_conf = {'bootstrap.servers': args.bootstrap_servers,
                     'key.serializer': avro_key_serializer,
                     'value.serializer': avro_value_serializer}

    producer = SerializingProducer(producer_conf)

    print("Producing user records to topic {}. ^C to exit.".format(topic))
    while True:
        # Serve on_delivery callbacks from previous calls to produce()
        producer.poll(0.0)
        try:
            user_id = input("Enter User ID: ")
            product_id = input("Enter Product ID: ")
            quoted_price = input("Enter price: ")
            quoted_quantity = int(input("Enter the desired quantity: "))
            user_note = input("Enter additional note: ")

            user_quote_key = UserQuoteKey(user_id=int(user_id))

            user_quote_value = UserQuoteValue(product_id=int(product_id),
                                              quoted_price=int(quoted_price),
                                              quoted_quantity=quoted_quantity,
                                              user_note=user_note)

            producer.produce(topic=topic, key=user_quote_key, value=user_quote_value,
                             on_delivery=delivery_report)
        except KeyboardInterrupt:
            break
        except ValueError:
            print("Invalid input, discarding record...")
            continue

    print("\nFlushing records...")
    producer.flush()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="SerializingProducer Example")
    parser.add_argument('-b', dest="bootstrap_servers", required=True,
                        help="Bootstrap broker(s) (host[:port])")
    parser.add_argument('-s', dest="schema_registry", required=True,
                        help="Schema Registry (http(s)://host[:port]")
    parser.add_argument('-t', dest="topic", default="example_serde_avro",
                        help="Topic name")

    main(parser.parse_args())
