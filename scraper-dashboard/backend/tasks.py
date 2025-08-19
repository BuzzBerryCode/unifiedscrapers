import os
import sys
import json
import asyncio
import time
from datetime import datetime
from celery import Celery
import redis
from supabase import create_client, Client

# Add the parent directory to path to import scrapers
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import the unified scrapers
try:
    from UnifiedScraper import process_instagram_user, process_tiktok_account, process_creator_media
    from UnifiedRescaper import rescrape_and_update_creator, get_existing_creators
except ImportError as e:
    print(f"Warning: Could not import scrapers: {e}")
    print("Make sure UnifiedScraper.py and UnifiedRescaper.py are in the parent directory")

# ==================== CONFIGURATION ====================

# Redis & Celery
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
redis_client = redis.from_url(REDIS_URL)

# Celery app
celery_app = Celery(
    "scraper_tasks",
    broker=REDIS_URL,
    backend=REDIS_URL
)

# Celery configuration
celery_app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    task_track_started=True,

    worker_prefetch_multiplier=1,
    task_acks_late=True,
    worker_disable_rate_limits=False,
    task_default_retry_delay=60,
    task_max_retries=3
)

# Supabase
SUPABASE_URL = os.getenv("SUPABASE_URL", "https://unovwhgnwenxbyvpevcz.supabase.co")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InVub3Z3aGdud2VueGJ5dnBldmN6Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1MjEzNjU0MCwiZXhwIjoyMDY3NzEyNTQwfQ.bqXWKTR64TRARH2VOrjiQdPnQ7W-8EGJp5RIi7yFmck")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ==================== HELPER FUNCTIONS ====================

def update_job_status(job_id: str, status: str, **kwargs):
    """Update job status in the database."""
    update_data = {
        "status": status,
        "updated_at": datetime.utcnow().isoformat(),
        **kwargs
    }
    
    try:
        supabase.table("scraper_jobs").update(update_data).eq("id", job_id).execute()
        print(f"Updated job {job_id}: {status}")
    except Exception as e:
        print(f"Error updating job {job_id}: {e}")

def update_job_progress(job_id: str, processed_items: int, failed_items: int = 0):
    """Update job progress."""
    try:
        supabase.table("scraper_jobs").update({
            "processed_items": processed_items,
            "failed_items": failed_items,
            "updated_at": datetime.utcnow().isoformat()
        }).eq("id", job_id).execute()
    except Exception as e:
        print(f"Error updating job progress {job_id}: {e}")

# ==================== CELERY TASKS ====================

@celery_app.task(bind=True)
def process_new_creators(self, job_id: str):
    """Process new creators from CSV data."""
    try:
        print(f"Starting job {job_id}: process_new_creators")
        update_job_status(job_id, "running")
        
        # Get CSV data from Redis
        csv_data_json = redis_client.get(f"job_data:{job_id}")
        if not csv_data_json:
            raise Exception("CSV data not found in Redis")
        
        csv_data = json.loads(csv_data_json)
        total_items = len(csv_data)
        processed_items = 0
        failed_items = 0
        results = {"added": [], "failed": [], "skipped": [], "filtered": []}
        niche_stats = {"primary_niches": {}, "secondary_niches": {}}
        
        print(f"Processing {total_items} creators")
        
        for i, creator_data in enumerate(csv_data):
            try:
                username = creator_data['Usernames'].strip()
                platform = creator_data['Platform'].lower()
                
                print(f"Processing {i+1}/{total_items}: @{username} ({platform})")
                
                # Check if creator already exists
                existing = supabase.table("creatordata").select("id", "platform", "primary_niche").eq("handle", username).execute()
                if existing.data:
                    existing_creator = existing.data[0]
                    existing_platform = existing_creator.get('platform', 'Unknown')
                    existing_niche = existing_creator.get('primary_niche', 'Unknown')
                    results["skipped"].append(f"@{username} - Already exists in database ({existing_platform}, {existing_niche} niche)")
                    processed_items += 1
                    continue
                
                # Process based on platform
                if platform == 'instagram':
                    result = process_instagram_user(username)
                    if result and isinstance(result, dict):
                        if 'error' in result:
                            # Handle different error types
                            if result['error'] == 'filtered':
                                results["filtered"].append(f"@{username} - {result['message']}")
                            else:
                                results["failed"].append(f"@{username} - {result['message']}")
                                failed_items += 1
                        elif 'creator_id' in result:
                            # Process media asynchronously
                            asyncio.run(process_creator_media(result['creator_id'], username, result['data']))
                            results["added"].append(f"@{username} (Instagram)")
                            
                            # Track niche statistics
                            if 'data' in result and result['data']:
                                primary_niche = result['data'].get('primary_niche')
                                secondary_niche = result['data'].get('secondary_niche')
                                
                                if primary_niche:
                                    niche_stats["primary_niches"][primary_niche] = niche_stats["primary_niches"].get(primary_niche, 0) + 1
                                if secondary_niche:
                                    niche_stats["secondary_niches"][secondary_niche] = niche_stats["secondary_niches"].get(secondary_niche, 0) + 1
                        else:
                            results["added"].append(f"@{username} (Instagram)")
                    elif result:
                        results["added"].append(f"@{username} (Instagram)")
                    else:
                        results["failed"].append(f"@{username} - failed to process")
                        failed_items += 1
                        
                elif platform == 'tiktok':
                    from UnifiedScraper import SCRAPECREATORS_API_KEY
                    influencer_data = process_tiktok_account(username, SCRAPECREATORS_API_KEY)
                    if influencer_data:
                        response = supabase.table("creatordata").insert(influencer_data).execute()
                        if response.data:
                            creator_id = response.data[0].get('id')
                            if creator_id:
                                asyncio.run(process_creator_media(creator_id, username, influencer_data))
                        results["added"].append(f"@{username} (TikTok)")
                        
                        # Track niche statistics
                        primary_niche = influencer_data.get('primary_niche')
                        secondary_niche = influencer_data.get('secondary_niche')
                        
                        if primary_niche:
                            niche_stats["primary_niches"][primary_niche] = niche_stats["primary_niches"].get(primary_niche, 0) + 1
                        if secondary_niche:
                            niche_stats["secondary_niches"][secondary_niche] = niche_stats["secondary_niches"].get(secondary_niche, 0) + 1
                    else:
                        results["failed"].append(f"@{username} - failed to process")
                        failed_items += 1
                
                processed_items += 1
                
                # Update progress every 5 items
                if i % 5 == 0:
                    update_job_progress(job_id, processed_items, failed_items)
                
            except Exception as e:
                print(f"Error processing @{username}: {e}")
                results["failed"].append(f"@{username} - {str(e)}")
                failed_items += 1
                processed_items += 1
        
        # Clean up Redis data
        redis_client.delete(f"job_data:{job_id}")
        
        print(f"Job {job_id} completed: {len(results['added'])} added, {len(results['failed'])} failed, {len(results['skipped'])} skipped")
        
        # Log niche statistics
        if niche_stats["primary_niches"] or niche_stats["secondary_niches"]:
            print("\n📊 NICHE BREAKDOWN:")
            print("Primary Niches:")
            for niche, count in sorted(niche_stats["primary_niches"].items(), key=lambda x: x[1], reverse=True):
                print(f"  • {niche}: {count} creators")
            
            if niche_stats["secondary_niches"]:
                print("Secondary Niches:")
                for niche, count in sorted(niche_stats["secondary_niches"].items(), key=lambda x: x[1], reverse=True):
                    print(f"  • {niche}: {count} creators")
        
        # Add niche stats to results for frontend display
        results["niche_stats"] = niche_stats
        
        # Final job completion update
        update_job_status(
            job_id,
            "completed",
            processed_items=processed_items,
            failed_items=failed_items,
            results=results
        )
        
        # Start next queued job
        try:
            from main import start_next_queued_job
            start_next_queued_job()
        except Exception as e:
            print(f"Error starting next queued job: {e}")
        
        return {
            "status": "completed",
            "processed": processed_items,
            "failed": failed_items,
            "results": results
        }
        
    except Exception as e:
        print(f"Job {job_id} failed: {e}")
        update_job_status(job_id, "failed", error_message=str(e))
        raise

@celery_app.task(bind=True)
def rescrape_all_creators(self, job_id: str):
    """Rescrape all creators in the database."""
    try:
        print(f"Starting job {job_id}: rescrape_all_creators")
        update_job_status(job_id, "running")
        
        # Get all creators
        existing_creators = get_existing_creators()
        total_items = len(existing_creators)
        processed_items = 0
        failed_items = 0
        results = {"updated": [], "deleted": [], "failed": []}
        
        print(f"Rescraping {total_items} creators")
        
        for i, creator in enumerate(existing_creators):
            try:
                handle = creator.get('handle')
                platform = creator.get('platform')
                
                print(f"Rescraping {i+1}/{total_items}: @{handle} ({platform})")
                
                # Rescrape the creator
                result = asyncio.run(rescrape_and_update_creator(creator))
                
                if result['status'] == 'success':
                    results["updated"].append(f"@{handle}")
                elif result['status'] == 'deleted':
                    results["deleted"].append(f"@{handle} - inactive")
                else:
                    results["failed"].append(f"@{handle} - {result.get('error', 'unknown error')}")
                    failed_items += 1
                
                processed_items += 1
                
                # Update progress every 10 items
                if i % 10 == 0:
                    update_job_progress(job_id, processed_items, failed_items)
                
            except Exception as e:
                print(f"Error rescraping @{creator.get('handle', 'unknown')}: {e}")
                results["failed"].append(f"@{creator.get('handle', 'unknown')} - {str(e)}")
                failed_items += 1
                processed_items += 1
        
        # Final update
        update_job_status(
            job_id,
            "completed",
            processed_items=processed_items,
            failed_items=failed_items,
            results=results
        )
        
        print(f"Job {job_id} completed: {len(results['updated'])} updated, {len(results['deleted'])} deleted, {len(results['failed'])} failed")
        
        # Start next queued job
        try:
            from main import start_next_queued_job
            start_next_queued_job()
        except Exception as e:
            print(f"Error starting next queued job: {e}")
        
        return {
            "status": "completed",
            "processed": processed_items,
            "failed": failed_items,
            "results": results
        }
        
    except Exception as e:
        print(f"Job {job_id} failed: {e}")
        update_job_status(job_id, "failed", error_message=str(e))
        raise

@celery_app.task(bind=True)
def rescrape_platform_creators(self, job_id: str, platform: str, resume_from_index: int = 0):
    """Rescrape creators for a specific platform with resume functionality."""
    try:
        print(f"Starting job {job_id}: rescrape_platform_creators ({platform})")
        if resume_from_index > 0:
            print(f"🔄 RESUMING from index {resume_from_index}")
        
        update_job_status(job_id, "running")
        
        # Get creators for the platform
        target_niches = ['Trading', 'Crypto', 'Finance']
        response = supabase.table("creatordata").select("*").in_('primary_niche', target_niches).eq('platform', platform.title()).execute()
        all_creators = response.data
        
        # Resume from specific index if provided
        creators = all_creators[resume_from_index:] if resume_from_index > 0 else all_creators
        
        total_items = len(all_creators)  # Total for progress tracking
        processed_items = resume_from_index  # Start from resume point
        failed_items = 0
        results = {"updated": [], "deleted": [], "failed": []}
        
        print(f"Rescraping {len(creators)} {platform} creators (starting from {resume_from_index + 1}/{total_items})")
        
        for i, creator in enumerate(creators):
            try:
                handle = creator.get('handle')
                current_index = resume_from_index + i
                
                print(f"Rescraping {current_index + 1}/{total_items}: @{handle} ({platform})")
                
                # Add rate limiting between creators
                if i > 0:  # Don't delay on the first creator
                    time.sleep(1)  # 1 second between creators to avoid rate limits
                
                # Add timeout protection to individual creator processing
                start_time = time.time()
                try:
                    # Use asyncio.wait_for with more aggressive timeout
                    result = asyncio.run(
                        asyncio.wait_for(
                            rescrape_and_update_creator(creator), 
                            timeout=120  # Reduced to 2 minute timeout per creator
                        )
                    )
                except asyncio.TimeoutError:
                    processing_time = time.time() - start_time
                    print(f"⏰ TIMEOUT: @{handle} processing exceeded 2 minutes ({processing_time:.2f}s)")
                    result = {'status': 'error', 'error': f'Processing timeout after {processing_time:.2f}s'}
                except Exception as e:
                    processing_time = time.time() - start_time
                    print(f"❌ CRITICAL ERROR: @{handle} processing failed after {processing_time:.2f}s: {e}")
                    result = {'status': 'error', 'error': f'Critical error: {str(e)}'}
                
                if result['status'] == 'success':
                    results["updated"].append(f"@{handle}")
                    print(f"✅ SUCCESS: @{handle} processed successfully")
                elif result['status'] == 'deleted':
                    results["deleted"].append(f"@{handle} - inactive")
                    print(f"🗑️ DELETED: @{handle} removed (inactive)")
                else:
                    error_msg = result.get('error', 'unknown error')
                    results["failed"].append(f"@{handle} - {error_msg}")
                    failed_items += 1
                    
                    # Categorize errors for better debugging
                    if 'timeout' in error_msg.lower():
                        print(f"⏰ TIMEOUT ERROR: @{handle}")
                    elif 'rate limit' in error_msg.lower() or '429' in error_msg:
                        print(f"⏳ RATE LIMIT ERROR: @{handle}")
                    elif 'api' in error_msg.lower():
                        print(f"🌐 API ERROR: @{handle}")
                    elif 'database' in error_msg.lower() or 'supabase' in error_msg.lower():
                        print(f"💾 DATABASE ERROR: @{handle}")
                    else:
                        print(f"❌ UNKNOWN ERROR: @{handle} - {error_msg}")
                
                processed_items += 1
                
                # Update progress every item and checkpoint every 10
                update_job_progress(job_id, processed_items, failed_items)
                
                if i % 10 == 0:
                    print(f"📊 CHECKPOINT: Processed {processed_items}/{total_items} creators ({failed_items} failed)")
                    # Force database update for checkpoint
                    update_job_status(
                        job_id,
                        "running",
                        processed_items=processed_items,
                        failed_items=failed_items,
                        results=results
                    )
                
            except Exception as e:
                print(f"❌ Critical error rescraping @{creator.get('handle', 'unknown')}: {e}")
                results["failed"].append(f"@{creator.get('handle', 'unknown')} - Critical error: {str(e)}")
                failed_items += 1
                processed_items += 1
        
        # Final update
        update_job_status(
            job_id,
            "completed",
            processed_items=processed_items,
            failed_items=failed_items,
            results=results
        )
        
        print(f"Job {job_id} completed: {len(results['updated'])} updated, {len(results['deleted'])} deleted, {len(results['failed'])} failed")
        
        # Start next queued job
        try:
            from main import start_next_queued_job
            start_next_queued_job()
        except Exception as e:
            print(f"Error starting next queued job: {e}")
        
        return {
            "status": "completed",
            "processed": processed_items,
            "failed": failed_items,
            "results": results
        }
        
    except Exception as e:
        print(f"Job {job_id} failed: {e}")
        update_job_status(job_id, "failed", error_message=str(e))
        raise

# ==================== CELERY WORKER SETUP ====================

if __name__ == '__main__':
    celery_app.start()
