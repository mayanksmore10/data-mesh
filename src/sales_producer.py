import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from dotenv import load_dotenv
from kafka import KafkaProducer


# ==========================================================
# LOAD ENVIRONMENT
# ==========================================================

load_dotenv()

KAFKA_SERVER = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
)

KAFKA_TOPIC = os.getenv(
    "KAFKA_SALES_TOPIC",
    "sales-events",
)

POLL_INTERVAL = int(
    os.getenv(
        "POLL_INTERVAL",
        "10",
    )
)


# ==========================================================
# SAMPLE BUSINESS DATA
# ==========================================================

REGIONS = [
    "North",
    "South",
    "East",
    "West",
]

PRODUCTS = [
    "Laptop",
    "Monitor",
    "Keyboard",
    "Mouse",
    "Printer",
    "Headset",
]

CUSTOMERS = [
    "C001",
    "C002",
    "C003",
    "C004",
    "C005",
    "C006",
    "C007",
    "C008",
]


PRODUCT_PRICES = {
    "Laptop": 75000,
    "Monitor": 18000,
    "Keyboard": 2500,
    "Mouse": 1200,
    "Printer": 15000,
    "Headset": 3500,
}


# ==========================================================
# CREATE KAFKA PRODUCER
# ==========================================================

def create_producer():

    return KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,

        value_serializer=lambda value:
            json.dumps(value).encode("utf-8"),

        key_serializer=lambda key:
            key.encode("utf-8")
            if key
            else None,
    )


# ==========================================================
# GENERATE ONE SALES EVENT
# ==========================================================

def generate_sales_event():

    product = random.choice(PRODUCTS)

    quantity = random.randint(
        1,
        5,
    )

    base_price = PRODUCT_PRICES[product]

    amount = (
        base_price
        * quantity
        * random.uniform(
            0.90,
            1.10,
        )
    )

    event = {
        "event_id": str(uuid.uuid4()),

        "customer_id": random.choice(
            CUSTOMERS
        ),

        "product_id": product,

        "quantity": quantity,

        "amount": round(
            amount,
            2,
        ),

        "region": random.choice(
            REGIONS
        ),

        "event_time": datetime.now(
            timezone.utc
        ).isoformat(),
    }

    return event


# ==========================================================
# MAIN PRODUCER LOOP
# ==========================================================

def main():

    print("=" * 70)
    print("SALES DATA PRODUCT → KAFKA")
    print("=" * 70)

    print(
        f"Kafka server : {KAFKA_SERVER}"
    )

    print(
        f"Kafka topic  : {KAFKA_TOPIC}"
    )

    print(
        f"Poll interval: {POLL_INTERVAL} seconds"
    )

    print()

    producer = create_producer()

    try:

        while True:

            events = []

            # Generate 10 sales events
            for _ in range(10):

                event = generate_sales_event()

                events.append(
                    event
                )

                producer.send(
                    KAFKA_TOPIC,
                    key=event["event_id"],
                    value=event,
                )

            producer.flush()

            print(
                f"[SALES] Sent {len(events)} events."
            )

            print(
                "[SALES] Sample event:"
            )

            print(
                json.dumps(
                    events[0],
                    indent=2,
                )
            )

            print(
                f"[WAIT] Sleeping {POLL_INTERVAL} seconds..."
            )

            print()

            time.sleep(
                POLL_INTERVAL
            )

    except KeyboardInterrupt:

        print(
            "\n[STOP] Sales producer stopped."
        )

    finally:

        producer.close()


if __name__ == "__main__":

    main()