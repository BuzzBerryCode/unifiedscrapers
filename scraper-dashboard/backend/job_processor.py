#!/usr/bin/env python3
"""
Direct Job Processor - Bypasses Celery entirely
Runs jobs directly with full error handling and progress tracking
"""

import os
import sys
import asyncio
import time
import json
from datetime import datetime
from supabase import create_client, Client
import redis
import requests
import traceback

# Add current directory to Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

# Configuration
SUPABASE_URL = os.getenv("SUPABASE_URL", "https://unovwhgnwenxbyvpevcz.supabase.co")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InVub3Z3aGdud2VueGJ5dnBldmN6Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1MjEzNjU0MCwiZXhwIjoyMDY3NzEyNTQwfQ.bqXWKTR64TRARH2VOrjiQdPnQ7W-8EGJp5RIi7yFmck")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
SCRAPECREATORS_API_KEY = os.getenv("SCRAPECREATORS_API_KEY", "wjhGgI14NjNMUuXA92YWXjojozF2")

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

def process_creator_direct(username, platform, supabase):
    """Process a single creator directly with comprehensive error handling"""
    print(f"   🔄 Processing @{username} ({platform})")
    
    try:
        if platform == 'instagram':
            # Import and run Instagram processing
            from UnifiedScraper import process_instagram_user
            result = asyncio.run(
                asyncio.wait_for(
                    asyncio.to_thread(process_instagram_user, username),
                    timeout=300  # 5 minute timeout per creator
                )
            )
        elif platform == 'tiktok':
            # Import and run TikTok processing
            from UnifiedScraper import process_tiktok_account
            result = asyncio.run(
                asyncio.wait_for(
                    asyncio.to_thread(process_tiktok_account, username, SCRAPECREATORS_API_KEY),
                    timeout=300
                )
            )
        else:
            return {"status": "failed", "reason": f"Unknown platform: {platform}"}
        
        # Process the result
        if result and isinstance(result, dict):
            if result.get("error") == "filtered":
                return {"status": "filtered", "reason": result.get('message', 'Filtered')}
            elif result.get("error") == "api_error":
                return {"status": "failed", "reason": result.get('message', 'API Error')}
            elif 'data' in result and result['data']:
                return {"status": "added", "data": result['data']}
            else:
                return {"status": "failed", "reason": "Processing failed - no valid data"}
        else:
            return {"status": "failed", "reason": "No result returned"}
            
    except asyncio.TimeoutError:
        return {"status": "failed", "reason": "Timeout (5 minutes)"}
    except Exception as e:
        return {"status": "failed", "reason": f"Error: {str(e)}"}

def run_new_creators_job(job_id, resume_from_index=0):
    """Run new creators job directly"""
    print(f"🚀 Running New Creators Job Directly")
    print(f"   Job ID: {job_id}")
    print(f"   Resume from: {resume_from_index}")
    
    # Connect to services
    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        print("✅ Connected to Supabase")
    except Exception as e:
        print(f"❌ Supabase connection failed: {e}")
        return False
    
    try:
        redis_client = redis.from_url(REDIS_URL)
        csv_data_json = redis_client.get(f"job_data:{job_id}")
        if csv_data_json:
            csv_data = json.loads(csv_data_json)
            print(f"✅ Retrieved CSV data: {len(csv_data)} creators")
        else:
            print("❌ No CSV data found in Redis")
            return False
    except Exception as e:
        print(f"❌ Redis connection failed: {e}")
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
            
            # Process the creator
            creator_result = process_creator_direct(username, platform, supabase)
            
            # Categorize result
            if creator_result["status"] == "added":
                results["added"].append(f"@{username}")
                # Track niche stats
                if "data" in creator_result:
                    primary_niche = creator_result["data"].get('primary_niche')
                    secondary_niche = creator_result["data"].get('secondary_niche')
                    if primary_niche:
                        niche_stats["primary_niches"][primary_niche] = niche_stats["primary_niches"].get(primary_niche, 0) + 1
                    if secondary_niche:
                        niche_stats["secondary_niches"][secondary_niche] = niche_stats["secondary_niches"].get(secondary_niche, 0) + 1
            elif creator_result["status"] == "filtered":
                results["filtered"].append(f"@{username} - {creator_result['reason']}")
            elif creator_result["status"] == "failed":
                results["failed"].append(f"@{username} - {creator_result['reason']}")
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
            
            # Intelligent rate limiting
            if creator_result["status"] == "failed" and "api" in creator_result["reason"].lower():
                time.sleep(3)  # Longer delay after API errors
            else:
                time.sleep(0.5)  # Shorter delay for successful/filtered results
            
        except Exception as e:
            print(f"❌ Critical error processing creator {i}: {e}")
            traceback.print_exc()
            results["failed"].append(f"Creator {i} - Critical error: {str(e)}")
            failed_items += 1
            processed_items += 1
    
    # Final update
    results["niche_stats"] = niche_stats
    update_job_status(supabase, job_id, "completed", processed_items, failed_items, results)
    
    print(f"\n🎉 Job completed!")
    print(f"   Total processed: {processed_items}/{total_items}")
    print(f"   Added: {len(results['added'])}")
    print(f"   Skipped: {len(results['skipped'])}")
    print(f"   Filtered: {len(results['filtered'])}")
    print(f"   Failed: {len(results['failed'])}")
    
    return True

def main():
    """Main function to process the stuck job"""
    print("🚀 DIRECT JOB PROCESSOR")
    print("=" * 50)
    
    # The current stuck job
    job_id = "df295d3d-22ab-4dcd-91a5-f24838cee348"
    resume_from = 380
    
    print(f"🎯 Job ID: {job_id}")
    print(f"📍 Resume from: {resume_from}")
    print(f"📋 Remaining: {507 - resume_from} creators")
    
    success = run_new_creators_job(job_id, resume_from)
    
    if success:
        print("\n✅ Job completed successfully!")
    else:
        print("\n❌ Job failed!")

if __name__ == "__main__":
    main()
