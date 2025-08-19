#!/usr/bin/env python3
"""
Direct database approach to unstuck the new creators job
"""

import os
import sys
from supabase import create_client, Client
from celery import Celery
import redis
import json
from datetime import datetime

# Configuration from environment
SUPABASE_URL = os.getenv("SUPABASE_URL", "https://unovwhgnwenxbyvpevcz.supabase.co")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InVub3Z3aGdud2VueGJ5dnBldmN6Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1MjEzNjU0MCwiZXhwIjoyMDY3NzEyNTQwfQ.bqXWKTR64TRARH2VOrjiQdPnQ7W-8EGJp5RIi7yFmck")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

def get_supabase_client():
    """Get Supabase client"""
    try:
        return create_client(SUPABASE_URL, SUPABASE_KEY)
    except Exception as e:
        print(f"❌ Supabase connection failed: {e}")
        return None

def get_redis_client():
    """Get Redis client"""
    try:
        return redis.from_url(REDIS_URL)
    except Exception as e:
        print(f"❌ Redis connection failed: {e}")
        return None

def get_celery_app():
    """Get Celery app"""
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
        return celery_app
    except Exception as e:
        print(f"❌ Celery connection failed: {e}")
        return None

def main():
    print("🔧 Direct Job Unstuck Script")
    print("=" * 40)
    
    # Connect to Supabase
    supabase = get_supabase_client()
    if not supabase:
        print("❌ Cannot connect to Supabase")
        sys.exit(1)
    
    print("✅ Connected to Supabase")
    
    # Get all running jobs
    try:
        response = supabase.table("scraper_jobs").select("*").eq("status", "running").execute()
        jobs = response.data
        
        print(f"📋 Found {len(jobs)} running jobs:")
        
        rescraper_job = None
        new_creators_job = None
        
        for job in jobs:
            print(f"   Job: {job['id'][:8]}... - {job['description']}")
            print(f"        Progress: {job.get('processed_items', 0)}/{job.get('total_items', 0)}")
            print(f"        Created: {job['created_at']}")
            
            if 'rescrape' in job['description'].lower() and 'instagram' in job['description'].lower():
                rescraper_job = job
                print("        🎯 THIS IS THE RESCRAPER JOB")
            
            if 'crypto' in job['description'].lower() and 'trading' in job['description'].lower():
                new_creators_job = job
                print("        🎯 THIS IS THE NEW CREATORS JOB")
        
        # Cancel rescraper job if found
        if rescraper_job:
            print(f"\n❌ Cancelling rescraper job {rescraper_job['id'][:8]}...")
            supabase.table("scraper_jobs").update({
                "status": "cancelled",
                "updated_at": datetime.utcnow().isoformat()
            }).eq("id", rescraper_job['id']).execute()
            print("✅ Rescraper job cancelled")
        
        # Force continue new creators job if found
        if new_creators_job:
            job_id = new_creators_job['id']
            resume_from = new_creators_job.get('processed_items', 0)
            
            print(f"\n🚀 Force continuing new creators job {job_id[:8]}...")
            print(f"   Resuming from index: {resume_from}")
            
            # Connect to Celery
            celery_app = get_celery_app()
            if celery_app:
                print("✅ Connected to Celery")
                
                # Send the task to force continue
                try:
                    result = celery_app.send_task(
                        "tasks.process_new_creators", 
                        args=[job_id, resume_from]
                    )
                    print(f"✅ Celery task dispatched: {result.id}")
                    print(f"🚀 New creators job should now continue from position {resume_from}")
                    
                except Exception as e:
                    print(f"❌ Failed to dispatch Celery task: {e}")
                    print("Trying alternative approach...")
                    
                    # Alternative: Just update the job status and let the system pick it up
                    supabase.table("scraper_jobs").update({
                        "status": "running",
                        "updated_at": datetime.utcnow().isoformat()
                    }).eq("id", job_id).execute()
                    print("✅ Job status updated to running - system should pick it up")
            else:
                print("❌ Cannot connect to Celery, trying database approach...")
                
                # Just update the job to trigger restart
                supabase.table("scraper_jobs").update({
                    "status": "running",
                    "updated_at": datetime.utcnow().isoformat()
                }).eq("id", job_id).execute()
                print("✅ Job status refreshed - should trigger restart")
        
        if not new_creators_job:
            print("\n❌ No stuck new creators job found")
            print("Available jobs:")
            for job in jobs:
                print(f"   - {job['description']} ({job['status']})")
        
    except Exception as e:
        print(f"❌ Database error: {e}")
        sys.exit(1)
    
    print("\n🎉 Job management completed!")
    print("Check your dashboard - the new creators job should now be running")

if __name__ == "__main__":
    main()
