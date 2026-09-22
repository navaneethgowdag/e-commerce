
import argparse
import json
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path


# ============================================================
# DATE RANGE
# ============================================================

START_DATE = datetime(2011, 1, 1)
END_DATE = datetime(2026, 9, 22)


# ============================================================
# STATIC DATA POOLS
# ============================================================

EVENT_TYPES = [
    "page_view",
    "product_view",
    "search",
    "add_to_cart",
    "remove_from_cart",
    "purchase",
    "payment_failed",
]

DEVICES = [
    "mobile",
    "desktop",
    "tablet",
]

TRAFFIC_SOURCES = [
    "organic",
    "paid_search",
    "social",
    "email",
    "direct",
    "referral",
]

COUNTRIES = [
    "United States",
    "Canada",
    "United Kingdom",
    "Germany",
    "France",
    "Australia",
    "India",
]

COUNTRY_MARKET = {
    "United States": "US",
    "Canada": "Canada",
    "United Kingdom": "Europe",
    "Germany": "Europe",
    "France": "Europe",
    "Australia": "APAC",
    "India": "APAC",
}

REGIONS = [
    "West",
    "East",
    "Central",
    "South",
]

SEGMENTS = [
    "Consumer",
    "Corporate",
    "Home Office",
]

SHIP_MODES = [
    "Standard Class",
    "Second Class",
    "First Class",
    "Same Day",
]

ORDER_PRIORITIES = [
    "Low",
    "Medium",
    "High",
    "Critical",
]


# ============================================================
# PRODUCT CATALOG
# ============================================================

PRODUCTS = [
    {
        "id": "TEC-PHO-001",
        "name": "Smartphone",
        "category": "Technology",
        "sub_category": "Phones",
        "min_price": 200,
        "max_price": 1800,
    },
    {
        "id": "TEC-PHO-002",
        "name": "Business Smartphone",
        "category": "Technology",
        "sub_category": "Phones",
        "min_price": 300,
        "max_price": 1400,
    },
    {
        "id": "TEC-COM-001",
        "name": "Laptop",
        "category": "Technology",
        "sub_category": "Computers",
        "min_price": 500,
        "max_price": 2500,
    },
    {
        "id": "TEC-COM-002",
        "name": "Desktop Computer",
        "category": "Technology",
        "sub_category": "Computers",
        "min_price": 600,
        "max_price": 3000,
    },
    {
        "id": "TEC-ACC-001",
        "name": "Wireless Mouse",
        "category": "Technology",
        "sub_category": "Accessories",
        "min_price": 15,
        "max_price": 80,
    },
    {
        "id": "TEC-ACC-002",
        "name": "Mechanical Keyboard",
        "category": "Technology",
        "sub_category": "Accessories",
        "min_price": 40,
        "max_price": 180,
    },
    {
        "id": "TEC-PRI-001",
        "name": "Laser Printer",
        "category": "Technology",
        "sub_category": "Printers",
        "min_price": 100,
        "max_price": 800,
    },
    {
        "id": "TEC-PRI-002",
        "name": "Color Printer",
        "category": "Technology",
        "sub_category": "Printers",
        "min_price": 150,
        "max_price": 1000,
    },
    {
        "id": "FUR-CHA-001",
        "name": "Office Chair",
        "category": "Furniture",
        "sub_category": "Chairs",
        "min_price": 80,
        "max_price": 700,
    },
    {
        "id": "FUR-CHA-002",
        "name": "Executive Chair",
        "category": "Furniture",
        "sub_category": "Chairs",
        "min_price": 200,
        "max_price": 1200,
    },
    {
        "id": "FUR-TAB-001",
        "name": "Office Desk",
        "category": "Furniture",
        "sub_category": "Tables",
        "min_price": 150,
        "max_price": 1200,
    },
    {
        "id": "FUR-TAB-002",
        "name": "Conference Table",
        "category": "Furniture",
        "sub_category": "Tables",
        "min_price": 500,
        "max_price": 2500,
    },
    {
        "id": "FUR-BOO-001",
        "name": "Bookshelf",
        "category": "Furniture",
        "sub_category": "Bookcases",
        "min_price": 100,
        "max_price": 900,
    },
    {
        "id": "FUR-FUR-001",
        "name": "Desk Lamp",
        "category": "Furniture",
        "sub_category": "Furnishings",
        "min_price": 20,
        "max_price": 150,
    },
    {
        "id": "OFF-PAP-001",
        "name": "Copy Paper",
        "category": "Office Supplies",
        "sub_category": "Paper",
        "min_price": 5,
        "max_price": 60,
    },
    {
        "id": "OFF-BIN-001",
        "name": "Ring Binder",
        "category": "Office Supplies",
        "sub_category": "Binders",
        "min_price": 5,
        "max_price": 40,
    },
    {
        "id": "OFF-STO-001",
        "name": "Storage Box",
        "category": "Office Supplies",
        "sub_category": "Storage",
        "min_price": 10,
        "max_price": 100,
    },
    {
        "id": "OFF-LAB-001",
        "name": "Shipping Labels",
        "category": "Office Supplies",
        "sub_category": "Labels",
        "min_price": 5,
        "max_price": 50,
    },
    {
        "id": "OFF-ART-001",
        "name": "Office Art Set",
        "category": "Office Supplies",
        "sub_category": "Art",
        "min_price": 10,
        "max_price": 100,
    },
]


# ============================================================
# CUSTOMER DATA
# ============================================================

FIRST_NAMES = [
    "John",
    "Michael",
    "David",
    "James",
    "Robert",
    "William",
    "Daniel",
    "Thomas",
    "Christopher",
    "Matthew",
    "Andrew",
    "Joseph",
    "Sarah",
    "Jennifer",
    "Jessica",
    "Emily",
    "Emma",
    "Olivia",
    "Sophia",
    "Ava",
    "Mia",
    "Isabella",
]

LAST_NAMES = [
    "Smith",
    "Johnson",
    "Williams",
    "Brown",
    "Jones",
    "Miller",
    "Davis",
    "Wilson",
    "Anderson",
    "Taylor",
    "Thomas",
    "Moore",
    "Jackson",
    "Martin",
    "Lee",
    "Harris",
    "Clark",
    "Lewis",
    "Walker",
    "Hall",
]


# ============================================================
# DATE GENERATOR
# ============================================================

def random_timestamp():
    """
    Generate a completely random date and time.

    The date is generated mathematically.
    Nothing is read from an external file.
    """

    number_of_days = (
        END_DATE - START_DATE
    ).days

    random_day = random.randint(
        0,
        number_of_days,
    )

    random_date = START_DATE + timedelta(
        days=random_day,
    )

    random_seconds = random.randint(
        0,
        86399,
    )

    return random_date + timedelta(
        seconds=random_seconds,
    )


# ============================================================
# RANDOM VALUE GENERATORS
# ============================================================

def random_customer_name():
    return (
        random.choice(FIRST_NAMES)
        + " "
        + random.choice(LAST_NAMES)
    )


def random_product():
    return random.choice(PRODUCTS)


def random_price(product):
    return round(
        random.uniform(
            product["min_price"],
            product["max_price"],
        ),
        2,
    )


def random_quantity():
    return random.choices(
        population=[
            1,
            2,
            3,
            4,
            5,
            6,
            7,
            8,
            10,
        ],
        weights=[
            45,
            20,
            12,
            8,
            5,
            3,
            2,
            1,
            1,
        ],
        k=1,
    )[0]


def random_discount():
    return random.choices(
        population=[
            0,
            5,
            10,
            15,
            20,
            25,
            30,
            40,
            50,
        ],
        weights=[
            35,
            15,
            15,
            10,
            8,
            6,
            5,
            4,
            2,
        ],
        k=1,
    )[0]


# ============================================================
# EVENT GENERATOR
# ============================================================

def generate_event():

    timestamp = random_timestamp()

    event_type = random.choice(
        EVENT_TYPES
    )

    product = random_product()

    country = random.choice(
        COUNTRIES
    )

    market = COUNTRY_MARKET[
        country
    ]

    quantity = random_quantity()

    discount = random_discount()

    unit_price = random_price(
        product
    )

    sales = round(
        unit_price
        * quantity
        * (1 - discount / 100),
        2,
    )

    event = {
        "event_id": str(
            uuid.uuid4()
        ),

        "user_id": (
            f"user_"
            f"{random.randint(1, 100000):06d}"
        ),

        "session_id": str(
            uuid.uuid4()
        ),

        "event_type": event_type,

        "timestamp": timestamp.isoformat(),

        "event_date": timestamp.strftime(
            "%Y-%m-%d"
        ),

        "device": random.choice(
            DEVICES
        ),

        "traffic_source": random.choice(
            TRAFFIC_SOURCES
        ),

        "customer_name": (
            random_customer_name()
        ),

        "segment": random.choice(
            SEGMENTS
        ),

        "country": country,

        "market": market,

        "region": random.choice(
            REGIONS
        ),

        "product_id": product["id"],

        "category": product["category"],

        "sub_category": product[
            "sub_category"
        ],

        "product_name": product["name"],

        "product_price": unit_price,

        "quantity": quantity,

        "discount_percent": discount,

        "sales": sales,

        "ship_mode": random.choice(
            SHIP_MODES
        ),

        "order_priority": random.choice(
            ORDER_PRIORITIES
        ),
    }

    # --------------------------------------------------------
    # EVENT-SPECIFIC INFORMATION
    # --------------------------------------------------------

    if event_type == "page_view":

        event["page"] = random.choice([
            "home",
            "category",
            "product",
            "search",
            "cart",
            "checkout",
        ])

        event["duration_seconds"] = (
            random.randint(5, 900)
        )

    elif event_type == "product_view":

        event[
            "view_duration_seconds"
        ] = random.randint(
            5,
            600,
        )

    elif event_type == "search":

        event["search_query"] = random.choice([
            product["name"],
            product["category"],
            product["sub_category"],
            "office supplies",
            "laptop",
            "chair",
            "printer",
            "phone",
            "desk",
            "computer",
        ])

    elif event_type == "add_to_cart":

        event["cart_quantity"] = quantity

    elif event_type == "remove_from_cart":

        event["removed_quantity"] = (
            random.randint(
                1,
                quantity,
            )
        )

    elif event_type == "purchase":

        event["order_id"] = (
            f"ORD-"
            f"{random.randint(100000, 999999)}"
        )

        event["payment_method"] = (
            random.choice([
                "credit_card",
                "debit_card",
                "paypal",
                "upi",
                "net_banking",
            ])
        )

        event["purchase_status"] = (
            "completed"
        )

    elif event_type == "payment_failed":

        event["order_id"] = (
            f"ORD-"
            f"{random.randint(100000, 999999)}"
        )

        event["failure_reason"] = (
            random.choice([
                "insufficient_funds",
                "card_declined",
                "network_error",
                "invalid_card",
                "timeout",
            ])
        )

        event["purchase_status"] = (
            "failed"
        )

    return event


# ============================================================
# MAIN GENERATION LOOP
# ============================================================

def generate_events(
    number_of_events,
    output_file,
):

    output_path = Path(
        output_file
    )

    output_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    print()
    print("=" * 60)
    print("SYNTHETIC E-COMMERCE EVENT GENERATOR")
    print("=" * 60)
    print(
        f"Events     : {number_of_events:,}"
    )
    print(
        f"Date range : "
        f"{START_DATE.date()} -> "
        f"{END_DATE.date()}"
    )
    print(
        f"Output     : {output_path}"
    )
    print(
        "Excel      : NOT USED"
    )
    print("=" * 60)
    print()

    with open(
        output_path,
        "w",
        encoding="utf-8",
    ) as file:

        for i in range(
            number_of_events
        ):

            event = generate_event()

            file.write(
                json.dumps(
                    event,
                    separators=(",", ":"),
                )
                + "\n"
            )

            if (
                (i + 1) % 5000 == 0
                or i + 1 == number_of_events
            ):
                print(
                    f"Generated "
                    f"{i + 1:,} / "
                    f"{number_of_events:,}"
                )

    print()
    print(
        f"Successfully generated "
        f"{number_of_events:,} events."
    )
    print(
        f"File: {output_path}"
    )


# ============================================================
# CLI
# ============================================================

def main():

    parser = argparse.ArgumentParser(
        description=(
            "Generate synthetic "
            "e-commerce events."
        )
    )

    parser.add_argument(
        "--events",
        type=int,
        default=10000,
        help=(
            "Number of events "
            "to generate."
        ),
    )

    parser.add_argument(
        "--output",
        default="data/events.jsonl",
        help=(
            "Output JSONL file."
        ),
    )

    args = parser.parse_args()

    if args.events <= 0:
        raise ValueError(
            "--events must be greater than 0."
        )

    generate_events(
        number_of_events=args.events,
        output_file=args.output,
    )


if __name__ == "__main__":
    main()