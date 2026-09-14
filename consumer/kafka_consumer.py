import json
from confluent_kafka import Consumer, KafkaException
from spark.utils.logging_config import get_logger
from config.config import config

logger = get_logger("kafka_debug_consumer")

def main():
    # 1. Initialize Consumer configuration
    consumer_config = {
        'bootstrap.servers': config.KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'debug-consumer-group',
        'auto.offset.reset': 'earliest', # Start reading from the beginning if no offset exists
        'enable.auto.commit': True       # Automatically commit offsets after reading
    }
    
    consumer = Consumer(consumer_config)
    
    # 2. Subscribe to the topic
    consumer.subscribe([config.KAFKA_TOPIC])
    logger.info(f"Subscribed to topic: {config.KAFKA_TOPIC}")
    logger.info("Waiting for messages... (Press Ctrl+C to stop)")

    try:
        while True:
            # 3. Poll for messages (timeout in seconds)
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                continue
            if msg.error():
                logger.error(f"Consumer error: {msg.error()}")
                continue

            # 4. Extract and print message details
            event_value = msg.value().decode('utf-8')
            
            # Pretty-print the JSON for readability
            try:
                parsed_event = json.loads(event_value)
                logger.info(
                    f"Partition: {msg.partition()} | "
                    f"Offset: {msg.offset()} | "
                    f"Event Type: {parsed_event.get('event_type')} | "
                    f"User: {parsed_event.get('user_id')}"
                )
            except json.JSONDecodeError:
                logger.warning(f"Received non-JSON message: {event_value}")

    except KeyboardInterrupt:
        logger.info("Consumer interrupted by user (Ctrl+C).")
    finally:
        # 5. Graceful shutdown
        logger.info("Closing consumer...")
        consumer.close()
        logger.info("Consumer closed.")

if __name__ == "__main__":
    main()