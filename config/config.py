import os
from dotenv import load_dotenv

# Load .env file from the project root
load_dotenv()

class Config:
    # Kafka
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "ecommerce-events")
    
    # Paths
    PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    EVENTS_FILE = os.path.join(PROJECT_ROOT, "data", "events.jsonl")

config = Config()