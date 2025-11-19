# producer.py
import time
import json
import uuid
import random
from datetime import datetime, timezone

from kafka import KafkaProducer
from faker import Faker

fake = Faker()

def generate_synthetic_trip() -> dict:
    """
    Generates synthetic ride-sharing trip data.
    Domain: ride-sharing trips between major cities with realistic fields.
    """
    pickup_cities = ["New York", "Los Angeles", "Chicago", "Houston", "Miami"]
    dropoff_cities = ["New York", "Los Angeles", "Chicago", "Houston", "Miami"]
    statuses = ["requested", "ongoing", "completed", "cancelled"]
    payment_methods = ["Credit Card", "Cash", "Wallet", "Apple Pay", "Google Pay"]

    trip_id = str(uuid.uuid4())[:8]
    pickup_city = random.choice(pickup_cities)
    dropoff_city = random.choice(dropoff_cities)
    status = random.choices(
        statuses,
        weights=[0.15, 0.25, 0.5, 0.1],  # more completed trips
        k=1,
    )[0]
    distance_km = round(random.uniform(1, 30), 2)
    base_fare_per_km = random.uniform(1.2, 2.5)
    surge_multiplier = random.choice([1.0, 1.0, 1.2, 1.5])  # sometimes surge
    fare_amount = round(distance_km * base_fare_per_km * surge_multiplier, 2)

    payment_method = random.choice(payment_methods)
    driver_rating = round(random.uniform(3.2, 5.0), 1)
    ts = datetime.now(timezone.utc).isoformat()

    return {
        "trip_id": trip_id,
        "status": status,
        "pickup_city": pickup_city,
        "dropoff_city": dropoff_city,
        "distance_km": distance_km,
        "fare_amount": fare_amount,
        "timestamp": ts,
        "payment_method": payment_method,
        "driver_rating": driver_rating,
    }

def run_producer():
    """Kafka producer that sends synthetic ride-sharing trips to the 'trips' topic."""
    try:
        print("[Producer] Connecting to Kafka at localhost:9092...")
        producer = KafkaProducer(
            bootstrap_servers="localhost:9092",
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            request_timeout_ms=30000,
            max_block_ms=60000,
            retries=5,
        )
        print("[Producer] ✓ Connected to Kafka successfully!")

        count = 0
        while True:
            trip = generate_synthetic_trip()
            print(f"[Producer] Sending trip #{count}: {trip}")

            future = producer.send("trips", value=trip)
            record_metadata = future.get(timeout=10)
            print(
                f"[Producer] ✓ Sent to partition {record_metadata.partition} "
                f"at offset {record_metadata.offset}"
            )

            producer.flush()
            count += 1

            # Random delay between trips to simulate real-time streaming
            sleep_time = random.uniform(0.5, 2.0)
            time.sleep(sleep_time)

    except Exception as e:
        print(f"[Producer ERROR] {e}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    run_producer()
