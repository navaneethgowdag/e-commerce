import json
import sys

from confluent_kafka import Producer

from spark.utils.logging_config import get_logger
from spark.utils.validation import validate_jsonl_line
from config.config import config


logger = get_logger("kafka_producer")


def delivery_report(err, msg):

    if err is not None:
        logger.error(
            f"Message delivery failed: {err}"
        )

    else:
        if msg.offset() % 100 == 0:
            logger.info(
                f"Delivered to "
                f"{msg.topic()} "
                f"[{msg.partition()}] "
                f"@ offset {msg.offset()}"
            )


def main():

    producer_config = {
        "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
        "client.id": "ecommerce-producer",
    }

    producer = Producer(producer_config)

    logger.info(
        f"Connected to Kafka at "
        f"{config.KAFKA_BOOTSTRAP_SERVERS}"
    )

    events_sent = 0
    invalid_events = 0

    try:

        logger.info(
            f"Reading events from: "
            f"{config.EVENTS_FILE}"
        )

        with open(
            config.EVENTS_FILE,
            "r",
            encoding="utf-8"
        ) as file:

            for line_number, line in enumerate(
                file,
                start=1
            ):

                line = line.strip()

                if not line:
                    continue

                is_valid, event_dict, error_msg = (
                    validate_jsonl_line(line)
                )

                if not is_valid:

                    logger.warning(
                        f"Line {line_number} invalid: "
                        f"{error_msg}"
                    )

                    invalid_events += 1
                    continue

                event_json = json.dumps(
                    event_dict,
                    separators=(",", ":")
                )

                producer.produce(
                    topic=config.KAFKA_TOPIC,
                    key=event_dict["user_id"],
                    value=event_json,
                    callback=delivery_report,
                )

                events_sent += 1

                producer.poll(0)

        logger.info(
            "Flushing remaining messages..."
        )

        remaining = producer.flush(timeout=30)

        if remaining > 0:

            logger.error(
                f"{remaining} messages "
                f"were not delivered."
            )

            sys.exit(1)

        logger.info(
            f"Production complete. "
            f"Sent: {events_sent}, "
            f"Invalid: {invalid_events}"
        )

    except FileNotFoundError:

        logger.error(
            f"File not found: "
            f"{config.EVENTS_FILE}"
        )

        sys.exit(1)

    except KeyboardInterrupt:

        logger.info(
            "Production interrupted. "
            "Flushing remaining messages..."
        )

        producer.flush(timeout=10)

        sys.exit(1)

    except Exception as error:

        logger.exception(
            f"Unexpected error: {error}"
        )

        sys.exit(1)


if __name__ == "__main__":
    main()