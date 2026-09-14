import json
import time
import sys
from confluent_kafka import Producer, KafkaException
from spark.utils.logging_config import get_logger
from spark.utils.validation import validate_jsonl_line
from config.config import config

logger = get_logger("kafka_producer")

def delivery_report(err, msg):
    """
    Callback function triggered after a message is delivered or fails.
    """
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        # Only log every 100 messages to avoid console spam
        if msg.offset() % 100 == 0:
            logger.info(f"Delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")

def main():
    # 1. Initialize Producer with configuration
    producer_config = {
        'bootstrap.servers': config.KAFKA_BOOTSTRAP_SERVERS,
        'client.id': 'ecommerce-producer'
    }
    producer = Producer(producer_config)
    logger.info(f"Connected to Kafka at {config.KAFKA_BOOTSTRAP_SERVERS}")

    events_sent = 0
    invalid_events = 0

    try:
        logger.info(f"Reading events from: {config.EVENTS_FILE}")
        with open(config.EVENTS_FILE, 'r') as file:
            for line_number, line in enumerate(file, 1):
                line = line.strip()
                if not line:
                    continue

                # 2. Validate the JSON line
                is_valid, event_dict, error_msg = validate_jsonl_line(line)
                
                if not is_valid:
                    logger.warning(f"Line {line_number} invalid: {error_msg}")
                    invalid_events += 1
                    continue

                # 3. Produce the message
                # Key: user_id (ensures same user events go to same partition for ordering)
                # Value: the JSON string
                event_json = json.dumps(event_dict)
                
                producer.produce(
                    topic=config.KAFKA_TOPIC,
                    key=event_dict.get("user_id"), 
                    value=event_json,
                    callback=delivery_report
                )
                events_sent += 1

                # 4. Poll to trigger delivery callbacks and handle internal queues
                producer.poll(0)

        # 5. Flush remaining messages in the queue before exiting
        logger.info("Flushing remaining messages...")
        producer.flush(timeout=10)
        
        logger.info(f"✅ Production complete. Sent: {events_sent}, Invalid: {invalid_events}")

    except FileNotFoundError:
        logger.error(f"File not found: {config.EVENTS_FILE}. Run event_generator.py first.")
        sys.exit(1)
    except KeyboardInterrupt:
        logger.info("Production interrupted by user (Ctrl+C). Flushing...")
        producer.flush(timeout=5)
        logger.info("Flush complete. Exiting.")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()