#!/usr/bin/env python3
"""
Direct job runner that bypasses Celery and runs the job directly
"""

import os
import sys
import json
import asyncio
import time
from datetime import datetime
from supabase import create_client, Client
import redis

# Add the current directory to Python path so we can import our modules
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import our scraper functions
try:
    from UnifiedScraper import process_instagram_user, process_tiktok_account
except ImportError as e:
    print(f"❌ Failed to import scraper functions: {e}")
    print("Make sure UnifiedScraper.py is in the same directory")
    sys.exit(1)

# Configuration
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

def update_job_progress(supabase, job_id, processed_items, failed_items=0):
    """Update job progress in database"""
    try:
        supabase.table("scraper_jobs").update({
            "processed_items": processed_items,
            "failed_items": failed_items,
            "updated_at": datetime.utcnow().isoformat()
        }).eq("id", job_id).execute()
    except Exception as e:
        print(f"❌ Failed to update progress: {e}")

def update_job_status(supabase, job_id, status, processed_items=None, failed_items=None, results=None, error_message=None):
    """Update job status in database"""
    try:
        update_data = {
            "status": status,
            "updated_at": datetime.utcnow().isoformat()
        }
        
        if processed_items is not None:
            update_data["processed_items"] = processed_items
        if failed_items is not None:
            update_data["failed_items"] = failed_items
        if results is not None:
            update_data["results"] = results
        if error_message is not None:
            update_data["error_message"] = error_message
            
        supabase.table("scraper_jobs").update(update_data).eq("id", job_id).execute()
        print(f"✅ Job status updated to: {status}")
    except Exception as e:
        print(f"❌ Failed to update job status: {e}")

def run_new_creators_job_directly(job_id, resume_from_index=0):
    """Run the new creators job directly without Celery"""
    print(f"🚀 Running new creators job directly")
    print(f"   Job ID: {job_id}")
    print(f"   Resume from: {resume_from_index}")
    
    # Connect to services
    supabase = get_supabase_client()
    if not supabase:
        return False
    
    redis_client = get_redis_client()
    if not redis_client:
        print("⚠️  Redis not available - continuing without it")
    
    try:
        # Get CSV data from Redis
        csv_data = None
        if redis_client:
            csv_data_json = redis_client.get(f"job_data:{job_id}")
            if csv_data_json:
                csv_data = json.loads(csv_data_json)
                print(f"✅ Retrieved CSV data from Redis: {len(csv_data)} creators")
        
        if not csv_data:
            print("❌ No CSV data found in Redis - job cannot continue")
            update_job_status(supabase, job_id, "failed", error_message="CSV data not found in Redis")
            return False
        
        # Resume from specific index
        if resume_from_index > 0:
            csv_data = csv_data[resume_from_index:]
            print(f"📋 Resuming from creator {resume_from_index + 1}")
        
        total_items = len(csv_data) + resume_from_index
        processed_items = resume_from_index
        failed_items = 0
        results = {"added": [], "failed": [], "skipped": [], "filtered": []}
        niche_stats = {"primary_niches": {}, "secondary_niches": {}}
        
        # Job-level timeout protection
        job_start_time = time.time()
        job_timeout = 4 * 60 * 60  # 4 hours
        last_progress_time = job_start_time
        
        print(f"🔄 Processing {len(csv_data)} creators (starting from {resume_from_index + 1}/{total_items})")
        
        for i, creator_data in enumerate(csv_data):
            try:
                # Check job-level timeout
                current_time = time.time()
                if current_time - job_start_time > job_timeout:
                    print(f"🚨 JOB TIMEOUT: Job exceeded {job_timeout/3600:.1f} hour limit")
                    results["failed"].append(f"Job timeout after {(current_time - job_start_time)/3600:.1f} hours")
                    break
                
                # Check for stuck job (no progress for 10 minutes)
                if current_time - last_progress_time > 600:  # 10 minutes
                    print(f"🚨 STUCK JOB DETECTED: No progress for {(current_time - last_progress_time)/60:.1f} minutes")
                    results["failed"].append(f"Job stuck - no progress for {(current_time - last_progress_time)/60:.1f} minutes")
                    break
                
                username = creator_data['Usernames'].strip()
                platform = creator_data['Platform'].lower()
                current_index = resume_from_index + i
                
                print(f"Processing {current_index + 1}/{total_items}: @{username} ({platform})")
                last_progress_time = current_time
                
                # Check if creator already exists
                existing = supabase.table("creatordata").select("id", "platform", "primary_niche").eq("handle", username).execute()
                if existing.data:
                    existing_creator = existing.data[0]
                    existing_platform = existing_creator.get('platform', 'Unknown')
                    existing_niche = existing_creator.get('primary_niche', 'Unknown')
                    results["skipped"].append(f"@{username} - Already exists in database ({existing_platform}, {existing_niche} niche)")
                    processed_items += 1
                    continue
                
                # Process based on platform with timeout protection
                try:
                    if platform == 'instagram':
                        # Use asyncio timeout for individual creator processing
                        result = asyncio.run(
                            asyncio.wait_for(
                                asyncio.to_thread(process_instagram_user, username),
                                timeout=180  # 3 minute timeout per creator
                            )
                        )
                    elif platform == 'tiktok':
                        result = asyncio.run(
                            asyncio.wait_for(
                                asyncio.to_thread(process_tiktok_account, username, os.getenv("SCRAPECREATORS_API_KEY")),
                                timeout=180  # 3 minute timeout per creator
                            )
                        )
                    else:
                        print(f"❌ Unknown platform: {platform}")
                        results["failed"].append(f"@{username} - Unknown platform: {platform}")
                        failed_items += 1
                        processed_items += 1
                        continue
                    
                    # Process result
                    if result and isinstance(result, dict):
                        if result.get("error") == "filtered":
                            results["filtered"].append(f"@{username} - {result.get('message', 'Filtered')}")
                        elif result.get("error") == "api_error":
                            results["failed"].append(f"@{username} - {result.get('message', 'API Error')}")
                            failed_items += 1
                        elif 'data' in result and result['data']:
                            results["added"].append(f"@{username}")
                            # Track niche stats
                            primary_niche = result['data'].get('primary_niche')
                            secondary_niche = result['data'].get('secondary_niche')
                            if primary_niche:
                                niche_stats["primary_niches"][primary_niche] = niche_stats["primary_niches"].get(primary_niche, 0) + 1
                            if secondary_niche:
                                niche_stats["secondary_niches"][secondary_niche] = niche_stats["secondary_niches"].get(secondary_niche, 0) + 1
                        else:
                            results["failed"].append(f"@{username} - Processing failed")
                            failed_items += 1
                    else:
                        results["failed"].append(f"@{username} - No result returned")
                        failed_items += 1
                    
                except asyncio.TimeoutError:
                    print(f"⏰ Timeout processing @{username}")
                    results["failed"].append(f"@{username} - Timeout (3 minutes)")
                    failed_items += 1
                except Exception as e:
                    print(f"❌ Error processing @{username}: {e}")
                    results["failed"].append(f"@{username} - Error: {str(e)}")
                    failed_items += 1
                
                processed_items += 1
                
                # Update progress every item
                update_job_progress(supabase, job_id, processed_items, failed_items)
                
                # Store intermediate results every 5 items
                if i % 5 == 0:
                    intermediate_results = {
                        "added": results["added"].copy(),
                        "failed": results["failed"].copy(),
                        "skipped": results["skipped"].copy(),
                        "filtered": results["filtered"].copy(),
                        "niche_stats": niche_stats.copy()
                    }
                    update_job_status(supabase, job_id, "running", processed_items, failed_items, intermediate_results)
                    print(f"📊 PROGRESS UPDATE: {processed_items}/{total_items} creators processed ({failed_items} failed)")
                
                # Rate limiting
                time.sleep(1)  # 1 second between creators
                
            except Exception as e:
                print(f"❌ Critical error processing creator {i}: {e}")
                results["failed"].append(f"Creator {i} - Critical error: {str(e)}")
                failed_items += 1
                processed_items += 1
        
        # Final update
        results["niche_stats"] = niche_stats
        update_job_status(supabase, job_id, "completed", processed_items, failed_items, results)
        
        print(f"\n🎉 Job completed!")
        print(f"   Added: {len(results['added'])}")
        print(f"   Skipped: {len(results['skipped'])}")
        print(f"   Filtered: {len(results['filtered'])}")
        print(f"   Failed: {len(results['failed'])}")
        
        return True
        
    except Exception as e:
        print(f"❌ Job failed with error: {e}")
        update_job_status(supabase, job_id, "failed", error_message=str(e))
        return False

def main():
    print("🚀 Direct Job Runner (Bypassing Celery)")
    print("=" * 50)
    
    # The job ID from our previous check
    job_id = "03368744-b56b-41c9-8c0d-fb2cf718aa96"
    resume_from = 396
    
    print(f"🎯 Running job: {job_id}")
    print(f"📍 Resuming from: {resume_from}")
    
    success = run_new_creators_job_directly(job_id, resume_from)
    
    if success:
        print("\n✅ Job completed successfully!")
    else:
        print("\n❌ Job failed!")

if __name__ == "__main__":
    main()
