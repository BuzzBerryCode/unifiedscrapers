#!/usr/bin/env python3
"""
Restart the failed new creators job from where it left off
"""

import os
import sys
from supabase import create_client, Client
from celery import Celery
from datetime import datetime

# Configuration
SUPABASE_URL = os.getenv("SUPABASE_URL", "https://unovwhgnwenxbyvpevcz.supabase.co")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InVub3Z3aGdud2VueGJ5dnBldmN6Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1MjEzNjU0MCwiZXhwIjoyMDY3NzEyNTQwfQ.bqXWKTR64TRARH2VOrjiQdPnQ7W-8EGJp5RIi7yFmck")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

def main():
    print("🚀 Restarting Failed New Creators Job")
    print("=" * 40)
    
    # Connect to Supabase
    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        print("✅ Connected to Supabase")
    except Exception as e:
        print(f"❌ Supabase connection failed: {e}")
        return
    
    # Find the failed new creators job
    job_id = "03368744-b56b-41c9-8c0d-fb2cf718aa96"  # From the previous output
    resume_from = 396  # Where it left off
    
    print(f"🎯 Restarting job: {job_id}")
    print(f"📍 Resuming from position: {resume_from}/507")
    
    try:
        # Update job status to running
        supabase.table("scraper_jobs").update({
            "status": "running",
            "updated_at": datetime.utcnow().isoformat()
        }).eq("id", job_id).execute()
        
        print("✅ Job status updated to 'running'")
        
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
            print(f"   Job will resume from creator {resume_from + 1}/507")
            print(f"   Remaining creators to process: {507 - resume_from}")
            
            print("\n🎉 SUCCESS!")
            print("The new creators job should now be running again.")
            print("Check your dashboard to see the progress!")
            
        except Exception as e:
            print(f"⚠️  Celery dispatch failed: {e}")
            print("Job status has been updated to 'running' - the system should pick it up automatically.")
            
    except Exception as e:
        print(f"❌ Failed to restart job: {e}")

if __name__ == "__main__":
    main()
