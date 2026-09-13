import json
from pathlib import Path

from kafka import KafkaProducer


KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "ecommerce-events"

EVENT_FILE = (
    Path(__file__).parent.parent
    / "data"
    / "events.jsonl"
)


def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,

        # Convert Python dictionary -> JSON bytes
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),

        # Wait for broker acknowledgement
        acks="all",

        # Retry failed sends
        retries=5,

        # Compress messages
        compression_type="gzip",

        # Batch messages
        linger_ms=10,
    )


def main():

    producer = create_producer()

    print("=" * 60)
    print("E-COMMERCE KAFKA PRODUCER")
    print("=" * 60)

    print(f"Broker : {KAFKA_BROKER}")
    print(f"Topic  : {KAFKA_TOPIC}")
    print(f"File   : {EVENT_FILE}")
    print()

    sent_count = 0


    try:
        with EVENT_FILE.open(
            "r",
            encoding="utf-8"
        ) as file:


            for line in file:

                line = line.strip()

                if not line:
                    continue

                event = json.loads(line)

                # Use user_id as Kafka key.
                # Events from the same user will be
                # routed consistently to the same partition.
                producer.send(
                    KAFKA_TOPIC,
                    key=event["user_id"].encode("utf-8"),
                    value=event,
                )

                sent_count += 1

                if sent_count % 100 == 0:
                    producer.flush()
                    print(f"Sent {sent_count} events")

        # Make sure all buffered events are delivered
        producer.flush()

        print()
        print(f"Finished. Total events sent: {sent_count}")

    except FileNotFoundError:

        print(f"ERROR: Event file not found:")
        print(EVENT_FILE)

    except json.JSONDecodeError as error:

        print(f"ERROR: Invalid JSON: {error}")

    except Exception as error:

        print(f"ERROR: {error}")

    finally:

        producer.close()


if __name__ == "__main__":
    main()