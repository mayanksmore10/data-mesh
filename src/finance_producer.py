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
    "KAFKA_FINANCE_TOPIC",
    "finance-events",
)

POLL_INTERVAL = int(
    os.getenv(
        "POLL_INTERVAL",
        "10",
    )
)


# ==========================================================
# SAMPLE FINANCE DATA
# ==========================================================

ACCOUNTS = [
    "ACC001",
    "ACC002",
    "ACC003",
    "ACC004",
    "ACC005",
    "ACC006",
]

TRANSACTION_TYPES = [
    "INCOME",
    "EXPENSE",
    "REFUND",
    "PAYMENT",
]

CURRENCIES = [
    "INR",
    "USD",
    "EUR",
]


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
# GENERATE ONE FINANCE EVENT
# ==========================================================

def generate_finance_event():

    transaction_type = random.choice(
        TRANSACTION_TYPES
    )

    amount = round(
        random.uniform(
            500,
            250000,
        ),
        2,
    )

    event = {
        "event_id": str(
            uuid.uuid4()
        ),

        "account_id": random.choice(
            ACCOUNTS
        ),

        "transaction_type": (
            transaction_type
        ),

        "amount": amount,

        "currency": random.choice(
            CURRENCIES
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
    print("FINANCE DATA PRODUCT → KAFKA")
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

            # Generate 10 finance events
            for _ in range(10):

                event = (
                    generate_finance_event()
                )

                events.append(event)

                producer.send(
                    KAFKA_TOPIC,
                    key=event["event_id"],
                    value=event,
                )

            producer.flush()

            print(
                f"[FINANCE] Sent "
                f"{len(events)} events."
            )

            print(
                "[FINANCE] Sample event:"
            )

            print(
                json.dumps(
                    events[0],
                    indent=2,
                )
            )

            print(
                f"[WAIT] Sleeping "
                f"{POLL_INTERVAL} seconds..."
            )

            print()

            time.sleep(
                POLL_INTERVAL
            )

    except KeyboardInterrupt:

        print(
            "\n[STOP] Finance producer stopped."
        )

    finally:

        producer.close()


if __name__ == "__main__":

    main()