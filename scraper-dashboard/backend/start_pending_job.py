#!/usr/bin/env python3
"""
Start the pending job and set it to resume from position 380
"""

import os
import sys
from supabase import create_client, Client
from celery import Celery
from datetime import datetime
import redis
import json

# Configuration
SUPABASE_URL = os.getenv("SUPABASE_URL", "https://unovwhgnwenxbyvpevcz.supabase.co")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InVub3Z3aGdud2VueGJ5dnBldmN6Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1MjEzNjU0MCwiZXhwIjoyMDY3NzEyNTQwfQ.bqXWKTR64TRARH2VOrjiQdPnQ7W-8EGJp5RIi7yFmck")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

def main():
    print("🚀 Starting Pending Job from Position 380")
    print("=" * 50)
    
    # Connect to Supabase
    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        print("✅ Connected to Supabase")
    except Exception as e:
        print(f"❌ Supabase connection failed: {e}")
        return
    
    # The new pending job ID
    job_id = "df295d3d-22ab-4dcd-91a5-f24838cee348"
    resume_from = 380  # Start from position 380 as requested
    
    print(f"🎯 Job ID: {job_id}")
    print(f"📍 Will start from position: {resume_from}/507")
    print(f"📋 Remaining creators to process: {507 - resume_from}")
    
    try:
        # Update job to set the resume position and status
        supabase.table("scraper_jobs").update({
            "status": "running",
            "processed_items": resume_from,  # Set starting position
            "updated_at": datetime.utcnow().isoformat()
        }).eq("id", job_id).execute()
        
        print(f"✅ Updated job status to 'running' starting from position {resume_from}")
        
        # Try to connect to Celery and dispatch the task
        try:
            celery_app = Celery('tasks')
            celery_app.config_from_object({
                'broker_url': REDIS_URL,
                'result_backend': REDIS_URL,
                'task_serializer': 'json',
                'accept_content': ['json'],
                'result_serializer': 'json',
                'timezone': 'UTC',
                'enable_utc': True,
            })
            
            print("✅ Connected to Celery")
            
            # Dispatch the task with resume index
            result = celery_app.send_task(
                "tasks.process_new_creators", 
                args=[job_id, resume_from]
            )
            
            print(f"🚀 Celery task dispatched successfully!")
            print(f"   Task ID: {result.id}")
            print(f"   Job will start from creator {resume_from + 1}/507")
            print(f"   Remaining creators to process: {507 - resume_from}")
            
            print("\n🎉 SUCCESS!")
            print("The new creators job should now be running from position 380.")
            print("Check your dashboard to see the progress!")
            
        except Exception as e:
            print(f"⚠️  Celery dispatch failed: {e}")
            print("Job status has been updated to 'running' - the system should pick it up automatically.")
            print("If it doesn't start within a few minutes, the server-side queue system may need attention.")
            
    except Exception as e:
        print(f"❌ Failed to start job: {e}")

if __name__ == "__main__":
    main()
