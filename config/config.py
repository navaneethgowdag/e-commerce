import os
from dataclasses import dataclass

from dotenv import load_dotenv


load_dotenv()


@dataclass(frozen=True)
class Config:
    KAFKA_BOOTSTRAP_SERVERS: str = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "localhost:9092",
    )

    KAFKA_TOPIC: str = os.getenv(
        "KAFKA_TOPIC",
        "ecommerce-events",
    )

    SPARK_CHECKPOINT_DIR: str = os.getenv(
        "SPARK_CHECKPOINT_DIR",
        "spark_checkpoint",
    )


config = Config()