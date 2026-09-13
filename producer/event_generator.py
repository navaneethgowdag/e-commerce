import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path


USERS = [
    f"user_{i:04d}"
    for i in range(1, 101)
]


PRODUCTS = [
    {
        "product_id": "prod_001",
        "name": "Laptop",
        "category": "Electronics",
        "price": 65000.00,
    },
    {
        "product_id": "prod_002",
        "name": "Smartphone",
        "category": "Electronics",
        "price": 32000.00,
    },
    {
        "product_id": "prod_003",
        "name": "Headphones",
        "category": "Electronics",
        "price": 3500.00,
    },
    {
        "product_id": "prod_004",
        "name": "Running Shoes",
        "category": "Footwear",
        "price": 4500.00,
    },
    {
        "product_id": "prod_005",
        "name": "Backpack",
        "category": "Accessories",
        "price": 1800.00,
    },
    {
        "product_id": "prod_006",
        "name": "Watch",
        "category": "Accessories",
        "price": 7500.00,
    },
    {
        "product_id": "prod_007",
        "name": "T-Shirt",
        "category": "Clothing",
        "price": 999.00,
    },
    {
        "product_id": "prod_008",
        "name": "Jeans",
        "category": "Clothing",
        "price": 2200.00,
    },
]


EVENT_TYPES = {
    "page_view": 0.35,
    "product_view": 0.30,
    "search": 0.12,
    "add_to_cart": 0.10,
    "remove_from_cart": 0.05,
    "purchase": 0.06,
    "payment_failed": 0.02,
}


DEVICES = [
    "mobile",
    "desktop",
    "tablet",
]


COUNTRIES = [
    "IN",
    "US",
    "UK",
    "CA",
    "AU",
]


SEARCH_TERMS = [
    "laptop",
    "phone",
    "headphones",
    "shoes",
    "backpack",
    "watch",
    "t-shirt",
    "jeans",
]


def generate_event():
    """Generate one e-commerce event."""

    user = random.choice(USERS)
    product = random.choice(PRODUCTS)

    event_type = random.choices(
        population=list(EVENT_TYPES.keys()),
        weights=list(EVENT_TYPES.values()),
        k=1,
    )[0]

    event = {
        "event_id": str(uuid.uuid4()),
        "user_id": user,
        "product_id": product["product_id"],
        "event_type": event_type,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "price": product["price"],
        "quantity": random.randint(1, 3),
        "device": random.choice(DEVICES),
        "country": random.choice(COUNTRIES),
    }

    if event_type == "search":
        event["search_term"] = random.choice(SEARCH_TERMS)

    return event


def generate_file(number_of_events):
    """Generate events.jsonl as UTF-8."""

    output_dir = Path(__file__).parent.parent / "data"
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / "events.jsonl"

    with output_file.open(
        "w",
        encoding="utf-8",
        newline="\n",
    ) as file:

        for _ in range(number_of_events):
            event = generate_event()

            file.write(
                json.dumps(event, ensure_ascii=False) + "\n"
            )

    print()
    print("=" * 60)
    print("EVENT GENERATION COMPLETE")
    print("=" * 60)
    print(f"Events : {number_of_events}")
    print(f"File   : {output_file}")
    print(f"Format : UTF-8 JSONL")


def stream_events(interval):
    """Continuously generate events to the terminal."""

    print("Starting e-commerce event stream...")
    print("Press Ctrl+C to stop.\n")

    try:

        while True:

            event = generate_event()

            print(
                json.dumps(
                    event,
                    ensure_ascii=False,
                ),
                flush=True,
            )

            time.sleep(interval)

    except KeyboardInterrupt:

        print("\nEvent generator stopped.")


def main():

    parser = argparse.ArgumentParser(
        description="E-commerce event generator"
    )

    parser.add_argument(
        "--events",
        type=int,
        default=0,
        help="Number of events to generate into data/events.jsonl.",
    )

    parser.add_argument(
        "--interval",
        type=float,
        default=1.0,
        help="Seconds between events in streaming mode.",
    )

    args = parser.parse_args()

    if args.events > 0:

        generate_file(args.events)

    else:

        stream_events(args.interval)


if __name__ == "__main__":
    main()