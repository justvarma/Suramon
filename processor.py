import time
import redis
import psycopg2
from dotenv import load_dotenv
import os

# Load environment variables from .env
load_dotenv()

# ---------------- REDIS CONNECTION ----------------
redis_client = redis.Redis(
    host=os.getenv("REDIS_HOST"),
    port=int(os.getenv("REDIS_PORT")),
    decode_responses=True
)

# ---------------- POSTGRES CONNECTION ----------------
conn = psycopg2.connect(
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT")
)
cursor = conn.cursor()

# ---------------- CONSTANTS ----------------
EVENT_TYPES = [
    "ORDER_DELAYED",
    "DELIVERY_FAILED",
    "INVENTORY_LOW",
    "VEHICLE_BREAKDOWN",
    "ROUTE_BLOCKED",
    "HUB_OVERLOAD"
]

HUBS = ["Delhi", "Mumbai", "Chennai", "Jaipur", "Hyderabad"]

SPIKE_THRESHOLD = 3        # number of events
CHECK_INTERVAL = 30        # seconds

print("🚀 Event Processor started successfully")

# ---------------- MAIN LOOP ----------------
while True:
    for event_type in EVENT_TYPES:
        for hub in HUBS:
            redis_key = f"event_count:{event_type}:{hub}"
            count = redis_client.get(redis_key)

            if count and int(count) >= SPIKE_THRESHOLD:
                message = f"Spike detected: {event_type} events at {hub}"

                cursor.execute(
                    """
                    INSERT INTO alert (hub, event_type, message)
                    VALUES (%s, %s, %s)
                    """,
                    (hub, event_type, message)
                )
                conn.commit()

                redis_client.set(f"hub_status:{hub}", "RED", ex=900)
                redis_client.delete(redis_key)

                print(f"🚨 ALERT GENERATED → {hub} | {event_type}")

    time.sleep(CHECK_INTERVAL)
