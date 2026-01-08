from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, List, Dict
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor
import redis
import os

app = FastAPI(title="Alert & Hub Status API")

# CORS for dashboard
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# PostgreSQL config with Docker-safe defaults
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "postgres"),
    "database": os.getenv("DB_NAME", "lemapdb"),
    "user": os.getenv("DB_USER", "lemap"),
    "password": os.getenv("DB_PASSWORD", "lemap123"),
    "port": os.getenv("DB_PORT", "5432")
}

# Redis config with Docker-safe defaults
REDIS_CONFIG = {
    "host": os.getenv("REDIS_HOST", "redis"),
    "port": int(os.getenv("REDIS_PORT", "6379")),
    "decode_responses": True
}

# Valid hubs (LOCKED)
VALID_HUBS = ["Delhi", "Mumbai", "Bangalore", "Chennai", "Hyderabad"]

def get_db_connection():
    """PostgreSQL connection with error handling"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB connection failed: {str(e)}")

def get_redis_connection():
    """Redis connection with error handling"""
    try:
        r = redis.Redis(**REDIS_CONFIG)
        r.ping()
        return r
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Redis connection failed: {str(e)}")

@app.get("/alerts")
async def get_alerts(hub: Optional[str] = Query(None, description="Filter by hub")):
    """
    GET /alerts - Read alerts from PostgreSQL
    Ordered by timestamp DESC
    Optional: filter by hub
    """
    # Validate hub if provided
    if hub and hub not in VALID_HUBS:
        raise HTTPException(status_code=400, detail=f"Invalid hub. Must be one of: {', '.join(VALID_HUBS)}")

    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    try:
        if hub:
            query = "SELECT id, hub, event_type, message, timestamp FROM alert WHERE hub = %s ORDER BY timestamp DESC"
            cursor.execute(query, (hub,))
        else:
            query = "SELECT id, hub, event_type, message, timestamp FROM alert ORDER BY timestamp DESC"
            cursor.execute(query)

        alerts = cursor.fetchall()

        # Convert timestamp to ISO format string
        for alert in alerts:
            if isinstance(alert['timestamp'], datetime):
                alert['timestamp'] = alert['timestamp'].isoformat()

        return alerts

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching alerts: {str(e)}")

    finally:
        cursor.close()
        conn.close()

@app.get("/hub-status")
async def get_hub_status():
    """
    GET /hub-status - Read hub health from Redis
    Returns color status for all hubs: "green" or "red"
    """
    try:
        r = get_redis_connection()

        status = {}
        for hub in VALID_HUBS:
            redis_key = f"hub_status:{hub}"
            hub_status = r.get(redis_key)

            # Default to green if not set
            status[hub] = hub_status if hub_status in ["green", "red"] else "green"

        return status

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching hub status: {str(e)}")

@app.get("/events")
async def get_events(hub: Optional[str] = Query(None, description="Filter by hub")):
    """
    GET /events - Read events from PostgreSQL
    Ordered by timestamp DESC
    Optional: filter by hub
    """
    # Validate hub if provided
    if hub and hub not in VALID_HUBS:
        raise HTTPException(status_code=400, detail=f"Invalid hub. Must be one of: {', '.join(VALID_HUBS)}")

    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    try:
        if hub:
            query = "SELECT id, event_type, hub, description, timestamp FROM event WHERE hub = %s ORDER BY timestamp DESC"
            cursor.execute(query, (hub,))
        else:
            query = "SELECT id, event_type, hub, description, timestamp FROM event ORDER BY timestamp DESC"
            cursor.execute(query)

        events = cursor.fetchall()

        # Convert timestamp to ISO format string
        for event in events:
            if isinstance(event['timestamp'], datetime):
                event['timestamp'] = event['timestamp'].isoformat()

        return events

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching events: {str(e)}")

    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)