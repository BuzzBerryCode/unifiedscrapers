#!/usr/bin/env python3
"""
Simple test script to check if Celery can connect to Redis and process jobs
"""
import os
from celery import Celery

# Get Redis URL from environment
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

print(f"Testing Celery connection to Redis: {REDIS_URL}")

# Create Celery app
celery_app = Celery(
    "test_scraper",
    broker=REDIS_URL,
    backend=REDIS_URL
)

# Test basic connection
try:
    # Check if we can connect to Redis
    i = celery_app.control.inspect()
    stats = i.stats()
    if stats:
        print("✅ Celery can connect to Redis!")
        print(f"Active workers: {list(stats.keys())}")
    else:
        print("❌ No active Celery workers found")
        
    # Try to get registered tasks
    registered = i.registered()
    if registered:
        print(f"Registered tasks: {registered}")
    else:
        print("No registered tasks found")
        
except Exception as e:
    print(f"❌ Celery connection failed: {e}")

# Test Redis connection directly
try:
    import redis
    redis_client = redis.from_url(REDIS_URL)
    redis_client.ping()
    print("✅ Direct Redis connection works!")
except Exception as e:
    print(f"❌ Direct Redis connection failed: {e}")
