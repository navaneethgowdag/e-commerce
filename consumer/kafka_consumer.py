import json

from kafka import KafkaConsumer


KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "ecommerce-events"
CONSUMER_GROUP = "ecommerce-analytics-consumer"


def create_consumer():

    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,

        # Get raw bytes first.
        # We will deserialize manually so that one
        # bad message doesn't crash the consumer.
        value_deserializer=lambda value: value,

        auto_offset_reset="earliest",

        group_id=CONSUMER_GROUP,

        enable_auto_commit=True,
    )


def main():

    consumer = create_consumer()

    print("=" * 60)
    print("E-COMMERCE KAFKA CONSUMER")
    print("=" * 60)

    print(f"Broker : {KAFKA_BROKER}")
    print(f"Topic  : {KAFKA_TOPIC}")
    print(f"Group  : {CONSUMER_GROUP}")
    print()
    print("Waiting for events...")
    print("Press Ctrl+C to stop.")
    print()

    try:

        for message in consumer:

            try:

                # bytes -> string
                raw_value = message.value.decode("utf-8")

                # string -> dictionary
                event = json.loads(raw_value)

                print(
                    f"partition={message.partition} "
                    f"offset={message.offset} "
                    f"user={event.get('user_id')} "
                    f"event={event.get('event_type')} "
                    f"product={event.get('product_id')}"
                )

            except (UnicodeDecodeError, json.JSONDecodeError) as error:

                print(
                    f"[INVALID JSON] "
                    f"partition={message.partition} "
                    f"offset={message.offset} "
                    f"error={error}"
                )

            except Exception as error:

                print(
                    f"[PROCESSING ERROR] "
                    f"partition={message.partition} "
                    f"offset={message.offset} "
                    f"error={error}"
                )

    except KeyboardInterrupt:

        print("\nConsumer stopped.")

    finally:

        consumer.close()


if __name__ == "__main__":
    main()