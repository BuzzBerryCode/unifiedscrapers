#!/usr/bin/env python3
"""
Simple Celery worker startup script for Railway deployment
"""
import os
import sys

def main():
    print("🚀 Starting Celery worker...")
    print(f"Redis URL: {os.getenv('REDIS_URL', 'Not set')}")
    
    try:
        # Import the celery app
        from tasks import celery_app
        
        # Start the worker
        celery_app.worker_main([
            'worker',
            '--loglevel=info',
            '--concurrency=2',
            '--max-tasks-per-child=1000'
        ])
        
    except Exception as e:
        print(f"❌ Error starting worker: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
