import sys
import os

# Add project root (e-commerce/) to the system path
PROJECT_ROOT = os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))
)

if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

import argparse
import json
import time
import uuid
import random

from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any

from faker import Faker

from spark.utils.logging_config import get_logger


logger = get_logger("event_generator")

fake = Faker()


# ---------------------------------------------------------------
# Event distribution
# ---------------------------------------------------------------
EVENT_WEIGHTS = [
    ("page_view", 40),
    ("product_view", 25),
    ("search", 15),
    ("add_to_cart", 10),
    ("remove_from_cart", 3),
    ("purchase", 5),
    ("payment_failed", 2),
]

EVENT_TYPES = [
    event
    for event, weight in EVENT_WEIGHTS
    for _ in range(weight)
]


# ---------------------------------------------------------------
# Static dimensions / random attributes
# ---------------------------------------------------------------
DEVICES = [
    "mobile",
    "desktop",
    "tablet",
]

COUNTRIES = [
    "US",
    "UK",
    "CA",
    "DE",
    "FR",
    "AU",
    "JP",
    "IN",
    "SG",
    "AE",
    "BR",
    "MX",
    "ES",
    "IT",
    "NL",
    "SE",
    "NO",
    "DK",
    "FI",
    "CH",
    "NZ",
    "ZA",
    "KR",
]

CATEGORIES = [
    "Electronics",
    "Clothing",
    "Footwear",
    "Home & Kitchen",
    "Beauty",
    "Sports",
    "Books",
    "Toys",
    "Grocery",
    "Accessories",
]

BRANDS = [
    "NovaTech",
    "UrbanEdge",
    "PrimeStyle",
    "HomeCraft",
    "FitZone",
    "GlowUp",
    "SoundMax",
    "TechPro",
    "EcoLife",
    "SmartGear",
    "TrendWear",
    "PowerPlus",
]

PRODUCT_NAMES = [
    "Wireless Headphones",
    "Bluetooth Speaker",
    "Smart Watch",
    "Gaming Mouse",
    "Mechanical Keyboard",
    "Running Shoes",
    "Casual T-Shirt",
    "Denim Jacket",
    "Backpack",
    "Coffee Maker",
    "Air Fryer",
    "Water Bottle",
    "Yoga Mat",
    "Fitness Band",
    "LED Desk Lamp",
    "Phone Case",
    "Portable Charger",
    "Sunglasses",
    "Face Cream",
    "Protein Powder",
    "Cookbook",
    "Action Figure",
    "Board Game",
    "Desk Organizer",
]

PAYMENT_METHODS = [
    "credit_card",
    "debit_card",
    "upi",
    "paypal",
    "apple_pay",
    "google_pay",
    "cash_on_delivery",
]

TRAFFIC_SOURCES = [
    "organic_search",
    "paid_search",
    "social_media",
    "email",
    "direct",
    "affiliate",
    "referral",
]

COUPON_CODES = [
    "WELCOME10",
    "SAVE20",
    "NEWUSER15",
    "FESTIVE25",
    "SUMMER10",
    "FLASH30",
    "VIP20",
    None,
    None,
    None,
]


# ---------------------------------------------------------------
# Random date range
# ---------------------------------------------------------------
START_DATE = datetime(
    2020,
    1,
    1,
    tzinfo=timezone.utc,
)

END_DATE = datetime(
    2026,
    12,
    31,
    23,
    59,
    59,
    tzinfo=timezone.utc,
)


def random_timestamp(
    start_date: datetime = START_DATE,
    end_date: datetime = END_DATE,
) -> str:
    """
    Generate a random UTC timestamp between start_date and end_date.
    """

    total_seconds = int(
        (end_date - start_date).total_seconds()
    )

    random_seconds = random.randint(
        0,
        total_seconds,
    )

    random_dt = start_date + timedelta(
        seconds=random_seconds
    )

    return random_dt.isoformat()


def generate_event() -> Dict[str, Any]:
    """
    Generate one realistic random e-commerce event.
    """

    event_type = random.choice(EVENT_TYPES)

    event_id = str(uuid.uuid4())

    user_id = f"usr_{random.randint(1000, 99999)}"

    product_id = f"prd_{random.randint(100, 9999)}"

    session_id = f"sess_{uuid.uuid4().hex[:12]}"

    order_id = None

    category = random.choice(CATEGORIES)

    brand = random.choice(BRANDS)

    product_name = random.choice(PRODUCT_NAMES)

    product_price = round(
        random.uniform(5.99, 1999.99),
        2,
    )

    discount_percent = random.choice(
        [0, 0, 0, 5, 10, 15, 20, 25, 30]
    )

    traffic_source = random.choice(
        TRAFFIC_SOURCES
    )

    event = {
        "event_id": event_id,

        "user_id": user_id,

        "session_id": session_id,

        "product_id": product_id,

        "event_type": event_type,

        "timestamp": random_timestamp(),

        "device": random.choice(DEVICES),

        "country": random.choice(COUNTRIES),

        "category": category,

        "brand": brand,

        "product_name": product_name,

        "product_price": product_price,

        "discount_percent": discount_percent,

        "traffic_source": traffic_source,
    }


    # -----------------------------------------------------------
    # Search event
    # -----------------------------------------------------------
    if event_type == "search":

        event["search_term"] = fake.word()


    # -----------------------------------------------------------
    # Cart / purchase related events
    # -----------------------------------------------------------
    if event_type in [
        "add_to_cart",
        "remove_from_cart",
        "purchase",
        "payment_failed",
    ]:

        event["quantity"] = random.randint(
            1,
            5,
        )

        event["price"] = round(
            product_price
            * (1 - discount_percent / 100),
            2,
        )


    # -----------------------------------------------------------
    # Purchase event
    # -----------------------------------------------------------
    if event_type == "purchase":

        order_id = f"ord_{uuid.uuid4().hex[:12]}"

        event["order_id"] = order_id

        event["payment_method"] = random.choice(
            PAYMENT_METHODS
        )

        event["coupon_code"] = random.choice(
            COUPON_CODES
        )

        event["shipping_cost"] = round(
            random.uniform(0, 30),
            2,
        )

        event["tax"] = round(
            event["price"]
            * random.uniform(0.02, 0.18),
            2,
        )

        event["total_amount"] = round(
            (
                event["price"]
                * event["quantity"]
            )
            + event["shipping_cost"]
            + event["tax"],
            2,
        )


    # -----------------------------------------------------------
    # Payment failure event
    # -----------------------------------------------------------
    if event_type == "payment_failed":

        event["payment_method"] = random.choice(
            PAYMENT_METHODS
        )

        event["failure_reason"] = random.choice([
            "insufficient_funds",
            "card_declined",
            "expired_card",
            "invalid_cvv",
            "bank_error",
            "network_error",
        ])


    # -----------------------------------------------------------
    # Add cart event
    # -----------------------------------------------------------
    if event_type == "add_to_cart":

        event["cart_id"] = f"cart_{uuid.uuid4().hex[:10]}"


    # -----------------------------------------------------------
    # Remove cart event
    # -----------------------------------------------------------
    if event_type == "remove_from_cart":

        event["cart_id"] = f"cart_{uuid.uuid4().hex[:10]}"

        event["removal_reason"] = random.choice([
            "changed_mind",
            "too_expensive",
            "added_by_mistake",
            "found_alternative",
            "other",
        ])


    return event


def generate_events(
    num_events: int,
    output_file: str,
    continuous: bool = False,
    interval: float = 1.0,
):
    """
    Generate events and write them to a JSONL file.
    """

    logger.info(
        f"Starting event generation. "
        f"Output: {output_file}"
    )

    try:

        # Ensure output directory exists
        output_dir = os.path.dirname(output_file)

        if output_dir:
            os.makedirs(
                output_dir,
                exist_ok=True,
            )


        with open(
            output_file,
            "a",
            encoding="utf-8",
        ) as f:

            count = 0

            while True:

                event = generate_event()

                f.write(
                    json.dumps(event)
                    + "\n"
                )

                # Flush immediately so another process
                # can consume the newly written event.
                f.flush()

                count += 1

                if count % 100 == 0:

                    logger.info(
                        f"Generated {count} events..."
                    )


                if not continuous:

                    if count >= num_events:
                        break

                else:

                    time.sleep(interval)


        logger.info(
            f"Successfully generated "
            f"{count} events to {output_file}"
        )


    except KeyboardInterrupt:

        logger.info(
            "Event generation interrupted "
            "by user (Ctrl+C)."
        )


    except Exception as e:

        logger.exception(
            f"Error during event generation: {e}"
        )


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="E-commerce Event Generator"
    )

    parser.add_argument(
        "--events",
        type=int,
        default=100,
        help="Number of events to generate",
    )

    parser.add_argument(
        "--output",
        type=str,
        default="data/events.jsonl",
        help="Output JSONL file",
    )

    parser.add_argument(
        "--continuous",
        action="store_true",
        help="Run continuously",
    )

    parser.add_argument(
        "--interval",
        type=float,
        default=0.5,
        help="Seconds between events",
    )

    args = parser.parse_args()

    generate_events(
        num_events=args.events,
        output_file=args.output,
        continuous=args.continuous,
        interval=args.interval,
    )