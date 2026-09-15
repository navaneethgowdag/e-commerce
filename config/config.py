import os
from dotenv import load_dotenv

# 1. Find the absolute path to the project root (e-commerce/)
# __file__ is config/config.py, so dirname twice gets us to the root
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# 2. Load the .env file from the project root
env_path = os.path.join(PROJECT_ROOT, ".env")
load_dotenv(dotenv_path=env_path)

class Config:
    """Centralized configuration loaded from environment variables."""
    
    # Kafka
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "ecommerce-events")
    
    # AWS S3 (We will use these in Phase 4/6)
    AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "")
    AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")
    S3_BUCKET = os.getenv("S3_BUCKET", "ecommerce-datalake-dev")
    
    # Paths
    EVENTS_FILE = os.path.join(PROJECT_ROOT, "data", "events.jsonl")
    SPARK_CHECKPOINT_DIR = os.path.join(PROJECT_ROOT, "data", "spark_checkpoints")

# Create a single instance to import elsewhere
config = Config()