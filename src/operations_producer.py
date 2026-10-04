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
    "KAFKA_OPERATIONS_TOPIC",
    "operations-events",
)

POLL_INTERVAL = int(
    os.getenv(
        "POLL_INTERVAL",
        "10",
    )
)


# ==========================================================
# SAMPLE OPERATIONS DATA
# ==========================================================

WAREHOUSES = [
    "WH001",
    "WH002",
    "WH003",
    "WH004",
    "WH005",
]

STATUSES = [
    "PROCESSING",
    "PACKED",
    "SHIPPED",
    "DELIVERED",
    "DELAYED",
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
# GENERATE ONE OPERATIONS EVENT
# ==========================================================

def generate_operations_event():

    status = random.choice(
        STATUSES
    )

    processing_time = round(
        random.uniform(
            5,
            180,
        ),
        2,
    )

    event = {
        "event_id": str(
            uuid.uuid4()
        ),

        "order_id": (
            f"ORD{random.randint(10000, 99999)}"
        ),

        "warehouse_id": random.choice(
            WAREHOUSES
        ),

        "status": status,

        "processing_time_minutes": (
            processing_time
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
    print("OPERATIONS DATA PRODUCT → KAFKA")
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

            # Generate 10 operations events
            for _ in range(10):

                event = (
                    generate_operations_event()
                )

                events.append(event)

                producer.send(
                    KAFKA_TOPIC,
                    key=event["event_id"],
                    value=event,
                )

            producer.flush()

            print(
                f"[OPERATIONS] Sent "
                f"{len(events)} events."
            )

            print(
                "[OPERATIONS] Sample event:"
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
            "\n[STOP] Operations producer stopped."
        )

    finally:

        producer.close()


if __name__ == "__main__":

    main()
    