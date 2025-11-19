# consumer.py
import json
import psycopg2
from kafka import KafkaConsumer

def run_consumer():
    """Consumes ride-sharing trips from Kafka and inserts them into PostgreSQL."""
    try:
        print("[Consumer] Connecting to Kafka at localhost:9092...")
        consumer = KafkaConsumer(
            "trips",
            bootstrap_servers="localhost:9092",
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            group_id="trips-consumer-group",
        )
        print("[Consumer] ✓ Connected to Kafka successfully!")

        print("[Consumer] Connecting to PostgreSQL...")
        conn = psycopg2.connect(
            dbname="kafka_db",
            user="kafka_user",
            password="kafka_password",
            host="localhost",
            port="5432",   # change to "5433" only if you remapped the port
        )
        conn.autocommit = True
        cur = conn.cursor()
        print("[Consumer] ✓ Connected to PostgreSQL successfully!")

        # Create trips table
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS trips (
                trip_id VARCHAR(50) PRIMARY KEY,
                status VARCHAR(50),
                pickup_city VARCHAR(100),
                dropoff_city VARCHAR(100),
                distance_km NUMERIC(6, 2),
                fare_amount NUMERIC(10, 2),
                timestamp TIMESTAMPTZ,
                payment_method VARCHAR(50),
                driver_rating NUMERIC(2, 1)
            );
            """
        )
        print("[Consumer] ✓ Table 'trips' ready.")
        print("[Consumer] 🎧 Listening for messages...\n")

        message_count = 0
        for message in consumer:
            try:
                trip = message.value

                insert_query = """
                    INSERT INTO trips (
                        trip_id, status, pickup_city, dropoff_city,
                        distance_km, fare_amount, timestamp,
                        payment_method, driver_rating
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (trip_id) DO NOTHING;
                """
                cur.execute(
                    insert_query,
                    (
                        trip["trip_id"],
                        trip["status"],
                        trip["pickup_city"],
                        trip["dropoff_city"],
                        trip["distance_km"],
                        trip["fare_amount"],
                        trip["timestamp"],
                        trip["payment_method"],
                        trip["driver_rating"],
                    ),
                )
                message_count += 1
                print(
                    f"[Consumer] ✓ #{message_count} Inserted trip {trip['trip_id']} | "
                    f"{trip['pickup_city']} → {trip['dropoff_city']} | "
                    f"${trip['fare_amount']} | {trip['status']}"
                )

            except Exception as e:
                print(f"[Consumer ERROR] Failed to process message: {e}")
                continue

    except Exception as e:
        print(f"[Consumer ERROR] {e}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    run_consumer()
