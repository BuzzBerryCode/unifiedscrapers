#!/bin/bash

# Kill any existing Celery workers to prevent conflicts
echo "Cleaning up any existing Celery workers..."
pkill -f "celery worker" || true

# Wait a moment for cleanup
sleep 2

# Start single Celery worker in background with better configuration
echo "Starting Celery worker..."
celery -A tasks worker --loglevel=info --concurrency=1 --max-tasks-per-child=10 --without-heartbeat --without-gossip &

# Wait for worker to start
sleep 5

# Start FastAPI app
echo "Starting FastAPI app..."
python entrypoint.py
