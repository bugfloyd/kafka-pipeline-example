#!/usr/bin/env python
import argparse

from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer
from message_objects import UserQuoteKey, UserQuoteValue


def dict_to_user_quote_key(obj, ctx):
    """
    Converts object literal(dict) to a User_Quote_Key instance.

    Args:
        obj (dict): Object literal(dict)

        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.

    """
    if obj is None:
        return None

    return UserQuoteKey(user_id=obj['user_id'])


def dict_to_user_quote_value(obj, ctx):
    """
    Converts object literal(dict) to a User_Quote_Value instance.

    Args:
        obj (dict): Object literal(dict)

        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.

    """
    if obj is None:
        return None

    return UserQuoteValue(product_id=obj['product_id'],
                          quoted_price=obj['quoted_price'],
                          quoted_quantity=obj['quoted_quantity'],
                          user_note=obj['user_note'])


def main(args):
    topic = args.topic

    key_schema_str = open('schema/KeySchema.avsc', "r").read()
    value_schema_str = open('schema/ValueSchema.avsc', "r").read()

    sr_conf = {'url': args.schema_registry}
    schema_registry_client = SchemaRegistryClient(sr_conf)

    avro_key_deserializer = AvroDeserializer(key_schema_str,
                                             schema_registry_client,
                                             dict_to_user_quote_key)
    avro_value_deserializer = AvroDeserializer(value_schema_str,
                                               schema_registry_client,
                                               dict_to_user_quote_value)

    consumer_conf = {'bootstrap.servers': args.bootstrap_servers,
                     'key.deserializer': avro_key_deserializer,
                     'value.deserializer': avro_value_deserializer,
                     'group.id': args.group,
                     'auto.offset.reset': "earliest"}

    consumer = DeserializingConsumer(consumer_conf)
    consumer.subscribe([topic])

    while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            user_quote = msg.value()
            if user_quote is not None:
                print("User {} Quote record: product_id: {}\n"
                      "\tquoted_price: {}\n"
                      "\tquoted_quantity: {}\n"
                      "\tuser_note: {}\n"
                      .format(msg.key().user_id, user_quote.product_id,
                              user_quote.quoted_price,
                              user_quote.quoted_quantity,
                              user_quote.user_note))
        except KeyboardInterrupt:
            break

    consumer.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Consumer Example client with "
                                                 "serialization capabilities")
    parser.add_argument('-b', dest="bootstrap_servers", required=True,
                        help="Bootstrap broker(s) (host[:port])")
    parser.add_argument('-s', dest="schema_registry", required=True,
                        help="Schema Registry (http(s)://host[:port]")
    parser.add_argument('-t', dest="topic", default="example_serde_avro",
                        help="Topic name")
    parser.add_argument('-g', dest="group", default="example_serde_avro",
                        help="Consumer group")

    main(parser.parse_args())
