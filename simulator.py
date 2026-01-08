import time
import random
import redis

redis_client = redis.Redis(
    host="localhost",
    port=6379,
    decode_responses=True
)

EVENT_TYPES = [
  "ORDER_DELAYED",
  "DELIVERY_FAILED",
  "INVENTORY_LOW",
  "VEHICLE_BREAKDOWN",
  "ROUTE_BLOCKED",
  "HUB_OVERLOAD"
]

HUBS = ["Delhi", "Mumbai", "Banglore", "Chennai", "Hyderabad"]

print("🎲 Event Simulator running...")

while True:
    event_type = random.choice(EVENT_TYPES)
    hub = random.choice(HUBS)

    key = f"event_count:{event_type}:{hub}"
    redis_client.incr(key)
    redis_client.expire(key, 600)

    print(f"📡 Generated → {hub} | {event_type}")

    time.sleep(10)
