#!/bin/bash

# Start Celery worker in background
echo "Starting Celery worker..."
python worker.py &

# Start FastAPI app
echo "Starting FastAPI app..."
python entrypoint.py
