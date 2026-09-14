import sys
import os

# Add the project root (e-commerce/) to the system path so we can import 'spark' and 'config' modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import argparse
import json
import time
import uuid
from datetime import datetime, timezone
from typing import List, Dict, Any
import random
from faker import Faker

from spark.utils.logging_config import get_logger

logger = get_logger("event_generator")
# ... rest of the code remains the same ...
fake = Faker()

# Weighted distribution for realistic e-commerce behavior
EVENT_WEIGHTS = [
    ("page_view", 40),
    ("product_view", 25),
    ("search", 15),
    ("add_to_cart", 10),
    ("remove_from_cart", 3),
    ("purchase", 5),
    ("payment_failed", 2)
]

EVENT_TYPES = [event for event, weight in EVENT_WEIGHTS for _ in range(weight)]

DEVICES = ["mobile", "desktop", "tablet"]
COUNTRIES = ["US", "UK", "CA", "DE", "FR", "AU", "JP"]

def generate_event() -> Dict[str, Any]:
    """Generates a single realistic e-commerce event."""
    event_type = random.choice(EVENT_TYPES)
    event_id = str(uuid.uuid4())
    user_id = f"usr_{random.randint(1000, 9999)}"
    product_id = f"prd_{random.randint(100, 999)}"
    
    event = {
        "event_id": event_id,
        "user_id": user_id,
        "product_id": product_id,
        "event_type": event_type,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "device": random.choice(DEVICES),
        "country": random.choice(COUNTRIES)
    }
    
    # Add conditional fields based on event type
    if event_type in ["add_to_cart", "remove_from_cart", "purchase", "payment_failed"]:
        event["price"] = round(random.uniform(9.99, 499.99), 2)
        event["quantity"] = random.randint(1, 5)
        
    if event_type == "search":
        event["search_term"] = fake.word()
        
    return event

def generate_events(num_events: int, output_file: str, continuous: bool = False, interval: float = 1.0):
    """
    Generates events and writes them to a JSONL file.
    """
    logger.info(f"Starting event generation. Output: {output_file}")
    
    try:
        with open(output_file, "a") as f:
            count = 0
            while True:
                event = generate_event()
                f.write(json.dumps(event) + "\n")
                count += 1
                
                if count % 100 == 0:
                    logger.info(f"Generated {count} events...")
                    
                if not continuous:
                    if count >= num_events:
                        break
                else:
                    time.sleep(interval)
                    
        logger.info(f"Successfully generated {count} events to {output_file}")
        
    except KeyboardInterrupt:
        logger.info("Event generation interrupted by user (Ctrl+C).")
    except Exception as e:
        logger.error(f"Error during event generation: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="E-commerce Event Generator")
    parser.add_argument("--events", type=int, default=100, help="Number of events to generate")
    parser.add_argument("--output", type=str, default="data/events.jsonl", help="Output file path")
    parser.add_argument("--continuous", action="store_true", help="Run continuously")
    parser.add_argument("--interval", type=float, default=0.5, help="Seconds between events (if continuous)")
    
    args = parser.parse_args()
    
    generate_events(
        num_events=args.events,
        output_file=args.output,
        continuous=args.continuous,
        interval=args.interval
    )