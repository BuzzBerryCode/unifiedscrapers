#!/usr/bin/env python3
"""
Check all jobs in the database to see current status
"""

import os
from supabase import create_client, Client
from datetime import datetime

# Configuration
SUPABASE_URL = os.getenv("SUPABASE_URL", "https://unovwhgnwenxbyvpevcz.supabase.co")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InVub3Z3aGdud2VueGJ5dnBldmN6Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1MjEzNjU0MCwiZXhwIjoyMDY3NzEyNTQwfQ.bqXWKTR64TRARH2VOrjiQdPnQ7W-8EGJp5RIi7yFmck")

def main():
    print("🔍 Checking All Jobs Status")
    print("=" * 40)
    
    # Connect to Supabase
    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        print("✅ Connected to Supabase")
    except Exception as e:
        print(f"❌ Supabase connection failed: {e}")
        return
    
    # Get all jobs ordered by creation date (newest first)
    try:
        response = supabase.table("scraper_jobs").select("*").order("created_at", desc=True).limit(10).execute()
        jobs = response.data
        
        print(f"📋 Found {len(jobs)} recent jobs:")
        print()
        
        for i, job in enumerate(jobs, 1):
            status = job['status']
            description = job['description']
            processed = job.get('processed_items', 0)
            total = job.get('total_items', 0)
            created = job['created_at']
            updated = job.get('updated_at', 'N/A')
            
            # Status emoji
            status_emoji = {
                'running': '🟢',
                'completed': '✅',
                'failed': '❌',
                'cancelled': '🚫',
                'pending': '⏳',
                'queued': '📋',
                'paused': '⏸️'
            }.get(status, '❓')
            
            print(f"{i}. {status_emoji} {status.upper()}")
            print(f"   ID: {job['id']}")
            print(f"   Description: {description}")
            print(f"   Progress: {processed}/{total} ({(processed/total*100) if total > 0 else 0:.1f}%)")
            print(f"   Created: {created}")
            print(f"   Updated: {updated}")
            
            # Identify specific jobs
            if 'crypto' in description.lower() and 'trading' in description.lower():
                print("   🎯 THIS IS THE NEW CREATORS JOB")
                
                if status == 'running' and processed < total:
                    print(f"   ⚠️  Job appears stuck at {processed}/{total}")
                    
                    # Try to restart it
                    print("   🚀 Attempting to restart...")
                    try:
                        supabase.table("scraper_jobs").update({
                            "updated_at": datetime.utcnow().isoformat()
                        }).eq("id", job['id']).execute()
                        print("   ✅ Job timestamp updated - should trigger restart")
                    except Exception as e:
                        print(f"   ❌ Failed to update job: {e}")
                        
            elif 'rescrape' in description.lower() and 'instagram' in description.lower():
                print("   🎯 THIS IS THE RESCRAPER JOB")
                
                if status == 'running':
                    print("   ⚠️  Rescraper still running - should be cancelled")
                    try:
                        supabase.table("scraper_jobs").update({
                            "status": "cancelled",
                            "updated_at": datetime.utcnow().isoformat()
                        }).eq("id", job['id']).execute()
                        print("   ✅ Rescraper job cancelled")
                    except Exception as e:
                        print(f"   ❌ Failed to cancel rescraper: {e}")
            
            print()
        
        # Check for any running jobs specifically
        running_response = supabase.table("scraper_jobs").select("*").eq("status", "running").execute()
        running_jobs = running_response.data
        
        print(f"🔍 Currently running jobs: {len(running_jobs)}")
        for job in running_jobs:
            print(f"   - {job['description']} ({job.get('processed_items', 0)}/{job.get('total_items', 0)})")
        
    except Exception as e:
        print(f"❌ Database error: {e}")

if __name__ == "__main__":
    main()
