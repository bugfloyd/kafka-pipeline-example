from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from confluent_kafka import avro


def get_schema_registry_client(url="http://localhost:8081"):
    return SchemaRegistryClient({"url": url})


def register_schema():
    client = get_schema_registry_client()

    key_schema = Schema(open('schema/KeySchema.avsc', "r").read(), schema_type="AVRO")
    key_schema_id = client.register_schema(
        subject_name="quote-feedback-key",
        schema=key_schema
    )
    print("Schema with subject {} registered with id: {}".format("quote-feedback-key", key_schema_id))

    value_schema = Schema(open('schema/ValueSchema.avsc', "r").read(), schema_type="AVRO")
    value_schema_id = client.register_schema(
        subject_name="quote-feedback-value",
        schema=value_schema
    )
    print("Schema with subject {} registered with id: {}".format("quote-feedback-value", value_schema_id))


def main():
    register_schema()


if __name__ == '__main__':
    main()
