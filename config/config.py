import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    KAFKA_BOOTSTRAP_SERVERS = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "localhost:9092"
    )

    KAFKA_TOPIC = os.getenv(
        "KAFKA_TOPIC",
        "ecommerce-events"
    )

    EVENTS_FILE = os.getenv(
        "EVENTS_FILE",
        "data/events.jsonl"
    )


config = Config()