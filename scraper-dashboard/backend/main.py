from fastapi import FastAPI, HTTPException, Depends, UploadFile, File, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import os
import pandas as pd
import io
from datetime import datetime, timedelta
import uuid
import asyncio
import random
from typing import List, Optional
import redis
import json
from supabase import create_client, Client
import jwt
from passlib.context import CryptContext
import sys
import traceback
import threading
import time
import signal
# Import simple scraper
from simple_scraper import get_scraper

# ==================== CONFIGURATION ====================

app = FastAPI(title="Scraper Dashboard API", version="1.0.0")

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "https://creatorscraper.vercel.app",
        "https://*.vercel.app",
        "*"  # Allow all origins for now - can restrict later
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Authentication
security = HTTPBearer()
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
SECRET_KEY = os.getenv("JWT_SECRET_KEY", "your-secret-key-change-this")
ALGORITHM = "HS256"

# Supabase - Initialize lazily to avoid startup failures
SUPABASE_URL = os.getenv("SUPABASE_URL", "https://unovwhgnwenxbyvpevcz.supabase.co")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InVub3Z3aGdud2VueGJ5dnBldmN6Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1MjEzNjU0MCwiZXhwIjoyMDY3NzEyNTQwfQ.bqXWKTR64TRARH2VOrjiQdPnQ7W-8EGJp5RIi7yFmck")
supabase: Client = None

def get_supabase_client():
    global supabase
    if supabase is None:
        try:
            supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
            print(f"✅ Supabase connected: {SUPABASE_URL}")
        except Exception as e:
            print(f"❌ Supabase connection failed: {e}")
            supabase = None
    return supabase

# Redis - Initialize lazily to avoid startup failures
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
redis_client = None

def get_redis_client():
    global redis_client
    if redis_client is None:
        try:
            redis_client = redis.from_url(REDIS_URL, decode_responses=True)
            redis_client.ping()
            print(f"✅ Redis connected: {REDIS_URL}")
        except Exception as e:
            print(f"❌ Redis connection failed: {e}")
            redis_client = None
    return redis_client

# ==================== JOB STATUS ENUM ====================

class JobStatus:
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    QUEUED = "queued"
    PAUSED = "paused"

# ==================== SIMPLE EXECUTION FUNCTIONS ====================

def simple_rescrape_creators(creator_handles: List[str]) -> Dict:
    """Simple function to rescrape a list of creators directly"""
    scraper = get_scraper()
    results = []
    successful = 0
    failed = 0
    
    print(f"🚀 Starting simple rescrape of {len(creator_handles)} creators")
    
    try:
        supabase = get_supabase_client()
        if not supabase:
            return {'status': 'error', 'error': 'Database not available'}
        
        for i, handle in enumerate(creator_handles, 1):
            print(f"\n[{i}/{len(creator_handles)}] Processing @{handle}")
            
            try:
                # Get existing creator data
                response = supabase.table("creatordata").select("*").eq("handle", handle).execute()
                if not response.data:
                    print(f"❌ Creator @{handle} not found in database")
                    failed += 1
                    continue
                
                existing_data = response.data[0]
                platform = existing_data.get('platform', '').lower()
                
                # Scrape new data
                if platform == 'instagram':
                    new_data = scraper.scrape_instagram_creator(handle)
                elif platform == 'tiktok':
                    new_data = scraper.scrape_tiktok_creator(handle)
                else:
                    print(f"❌ Unknown platform for @{handle}: {platform}")
                    failed += 1
                    continue
                
                if not new_data:
                    print(f"❌ Failed to scrape @{handle}")
                    failed += 1
                    continue
                
                if new_data.get('error') == 'temporary':
                    print(f"⚠️ Temporary error for @{handle} - skipping")
                    continue
                
                # Update creator
                update_result = scraper.update_existing_creator(handle, new_data, existing_data)
                if update_result['status'] == 'updated':
                    # Save to database
                    supabase.table("creatordata").update(new_data).eq("handle", handle).execute()
                    successful += 1
                    print(f"✅ Updated @{handle}")
                else:
                    print(f"❌ Failed to update @{handle}: {update_result.get('error', 'Unknown error')}")
                    failed += 1
                
                results.append({
                    'handle': handle,
                    'status': update_result['status'],
                    'error': update_result.get('error')
                })
                
                # Small delay to prevent API rate limiting
                time.sleep(0.5)
                
            except Exception as e:
                print(f"❌ Error processing @{handle}: {e}")
                failed += 1
                results.append({
                    'handle': handle,
                    'status': 'error',
                    'error': str(e)
                })
        
        summary = f"Completed: {successful} successful, {failed} failed"
        print(f"🏁 {summary}")
        
        return {
            'status': 'completed',
            'successful': successful,
            'failed': failed,
            'summary': summary,
            'results': results
        }
        
    except Exception as e:
        print(f"❌ Rescrape function failed: {e}")
        traceback.print_exc()
        return {'status': 'error', 'error': str(e)}

def simple_process_new_creators(csv_data: List[Dict]) -> Dict:
    """Simple function to process new creators from CSV data"""
    scraper = get_scraper()
    results = []
    successful = 0
    failed = 0
    skipped = 0
    
    print(f"🚀 Processing {len(csv_data)} new creators")
    
    try:
        supabase = get_supabase_client()
        if not supabase:
            return {'status': 'error', 'error': 'Database not available'}
        
        for i, creator_info in enumerate(csv_data, 1):
            handle = creator_info.get('handle', '').strip().lstrip('@')
            platform = creator_info.get('platform', '').lower()
            
            if not handle or not platform:
                print(f"❌ Missing handle or platform in row {i}")
                failed += 1
                continue
            
            print(f"\n[{i}/{len(csv_data)}] Processing new creator @{handle} ({platform})")
            
            try:
                # Check if creator already exists
                existing = supabase.table("creatordata").select("id").eq("handle", handle).execute()
                if existing.data:
                    print(f"⚠️ Creator @{handle} already exists - skipping")
                    skipped += 1
                    continue
                
                # Scrape creator data
                if platform == 'instagram':
                    creator_data = scraper.scrape_instagram_creator(handle)
                elif platform == 'tiktok':
                    creator_data = scraper.scrape_tiktok_creator(handle)
                else:
                    print(f"❌ Unsupported platform: {platform}")
                    failed += 1
                    continue
                
                if not creator_data:
                    print(f"❌ Failed to scrape @{handle}")
                    failed += 1
                    continue
                
                if creator_data.get('error') == 'temporary':
                    print(f"⚠️ Temporary error for @{handle} - skipping")
                    continue
                
                # Create new creator record
                create_result = scraper.create_new_creator(creator_data, creator_info)
                if create_result['status'] == 'created':
                    # Save to database
                    supabase.table("creatordata").insert(create_result['data']).execute()
                    successful += 1
                    print(f"✅ Created @{handle}")
                else:
                    print(f"❌ Failed to create @{handle}: {create_result.get('error', 'Unknown error')}")
                    failed += 1
                
                results.append({
                    'handle': handle,
                    'status': create_result['status'],
                    'error': create_result.get('error')
                })
                
                # Small delay to prevent API rate limiting
                time.sleep(0.5)
                
            except Exception as e:
                print(f"❌ Error processing @{handle}: {e}")
                failed += 1
                results.append({
                    'handle': handle,
                    'status': 'error',
                    'error': str(e)
                })
        
        summary = f"Completed: {successful} created, {skipped} skipped, {failed} failed"
        print(f"🏁 {summary}")
        
        return {
            'status': 'completed',
            'successful': successful,
            'skipped': skipped,
            'failed': failed,
            'summary': summary,
            'results': results
        }
        
    except Exception as e:
        print(f"❌ New creators function failed: {e}")
        traceback.print_exc()
        return {'status': 'error', 'error': str(e)}

# ==================== AUTHENTICATION ====================

def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    try:
        payload = jwt.decode(credentials.credentials, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise HTTPException(status_code=401, detail="Invalid token")
        return username
    except jwt.PyJWTError:
        raise HTTPException(status_code=401, detail="Invalid token")

# ==================== DIRECT JOB EXECUTION ====================

def start_job_directly(job_id: str, job_type: str):
    """Start a job directly in a background thread"""
    try:
        print(f"🚀 Starting job {job_id} directly (type: {job_type})")
        
        # Import tasks here to avoid circular imports
        try:
            if job_type == "new_creators":
                from tasks import process_new_creators
                process_new_creators(job_id)
            elif job_type == "rescrape_instagram":
                from tasks import rescrape_platform_creators
                rescrape_platform_creators(job_id, "instagram")
            elif job_type == "rescrape_tiktok":
                from tasks import rescrape_platform_creators
                rescrape_platform_creators(job_id, "tiktok")
            elif job_type == "rescrape_all":
                from tasks import rescrape_all_creators
                rescrape_all_creators(job_id)
            elif job_type == "daily_rescrape":
                from tasks import rescrape_all_creators
                rescrape_all_creators(job_id)  # Use same function, it handles auto-rescrape data
            elif job_type == "rescrape_overdue_all":
                from tasks import rescrape_all_creators
                rescrape_all_creators(job_id)  # Use same function, data comes from Redis
            elif job_type == "rescrape_overdue_instagram":
                from tasks import rescrape_platform_creators
                rescrape_platform_creators(job_id, "instagram")  # Filters to Instagram from Redis data
            elif job_type == "rescrape_overdue_tiktok":
                from tasks import rescrape_platform_creators
                rescrape_platform_creators(job_id, "tiktok")  # Filters to TikTok from Redis data
            elif job_type == "rescrape_todays_batch_all":
                from tasks import rescrape_all_creators
                rescrape_all_creators(job_id)  # Use same function, data comes from Redis
            elif job_type == "rescrape_todays_batch_instagram":
                from tasks import rescrape_platform_creators
                rescrape_platform_creators(job_id, "instagram")  # Filters to Instagram from Redis data
            elif job_type == "rescrape_todays_batch_tiktok":
                from tasks import rescrape_platform_creators
                rescrape_platform_creators(job_id, "tiktok")  # Filters to TikTok from Redis data
            elif job_type == "fix_corrupted_all":
                from tasks import rescrape_all_creators
                rescrape_all_creators(job_id)  # Use same function, data comes from Redis
            elif job_type == "fix_corrupted_instagram":
                from tasks import rescrape_platform_creators
                rescrape_platform_creators(job_id, "instagram")  # Filters to Instagram from Redis data
            elif job_type == "fix_corrupted_tiktok":
                from tasks import rescrape_platform_creators
                rescrape_platform_creators(job_id, "tiktok")  # Filters to TikTok from Redis data
            else:
                print(f"❌ Unknown job type: {job_type}")
                return
                
        except ImportError as import_error:
            print(f"❌ Failed to import tasks module: {import_error}")
            raise Exception(f"Tasks module import failed: {import_error}")
            
    except Exception as e:
        print(f"❌ Job {job_id} failed: {e}")
        traceback.print_exc()
        
        # Update job status to failed
        try:
            supabase = get_supabase_client()
            if supabase:
                supabase.table("scraper_jobs").update({
                    "status": JobStatus.FAILED,
                    "error_message": str(e),
                    "updated_at": datetime.utcnow().isoformat()
                }).eq("id", job_id).execute()
        except Exception as update_error:
            print(f"❌ Failed to update job status: {update_error}")

def check_running_jobs():
    """Check if there are any running jobs"""
    try:
        supabase = get_supabase_client()
        if not supabase:
            return False
            
        response = supabase.table("scraper_jobs").select("id", "created_at").eq("status", JobStatus.RUNNING).execute()
        
        # Clean up phantom jobs (running for more than 4 hours)
        if response.data:
            from datetime import datetime, timedelta
            current_time = datetime.utcnow()
            phantom_jobs = []
            
            for job in response.data:
                job_created = datetime.fromisoformat(job["created_at"].replace('Z', '+00:00')).replace(tzinfo=None)
                if current_time - job_created > timedelta(hours=4):
                    phantom_jobs.append(job["id"])
            
            # Mark phantom jobs as failed
            if phantom_jobs:
                print(f"🧹 Cleaning up {len(phantom_jobs)} phantom jobs: {phantom_jobs}")
                for phantom_id in phantom_jobs:
                    try:
                        supabase.table("scraper_jobs").update({
                            "status": JobStatus.FAILED,
                            "error_message": "Job stuck for >4 hours - marked as failed",
                            "updated_at": datetime.utcnow().isoformat()
                        }).eq("id", phantom_id).execute()
                    except Exception as cleanup_error:
                        print(f"⚠️ Failed to cleanup phantom job {phantom_id}: {cleanup_error}")
                
                # Re-check after cleanup
                response = supabase.table("scraper_jobs").select("id").eq("status", JobStatus.RUNNING).execute()
        
        return len(response.data) > 0
    except Exception as e:
        print(f"❌ Error checking running jobs: {e}")
        return False

def start_next_queued_job():
    """Start the next queued job if no jobs are running"""
    try:
        if check_running_jobs():
            print("⏳ Job already running, skipping queue check")
            return
            
        # Check if queue is paused
        redis_client = get_redis_client()
        if redis_client:
            try:
                queue_paused = redis_client.get("queue_paused")
                if queue_paused == "true":
                    print("⏸️ Queue is paused, skipping job start")
                    return
            except Exception as e:
                print(f"⚠️ Redis queue check failed: {e}")
        
        supabase = get_supabase_client()
        if not supabase:
            return
            
        # Get next pending or queued job
        response = supabase.table("scraper_jobs").select("*").in_("status", [JobStatus.PENDING, JobStatus.QUEUED]).order("created_at").limit(1).execute()
        
        if response.data:
            job = response.data[0]
            job_id = job["id"]
            job_type = job["job_type"]
            
            print(f"🎯 Starting next queued job: {job_id} (type: {job_type})")
            
            # Update status to running
            supabase.table("scraper_jobs").update({
                "status": JobStatus.RUNNING,
                "updated_at": datetime.utcnow().isoformat()
            }).eq("id", job_id).execute()
            
            # Start job in background thread
            thread = threading.Thread(target=start_job_directly, args=(job_id, job_type))
            thread.daemon = True
            thread.start()
            
    except Exception as e:
        print(f"❌ Error starting next queued job: {e}")
        traceback.print_exc()

# ==================== BACKGROUND JOB MONITOR ====================

def job_monitor():
    """Background thread to monitor and start pending jobs"""
    print("🔄 Starting background job monitor...")
    
    while True:
        try:
            # More aggressive queue checking to break phantom loops
            running_count = 0
            try:
                supabase = get_supabase_client()
                if supabase:
                    running_jobs = supabase.table("scraper_jobs").select("id").eq("status", JobStatus.RUNNING).execute()
                    running_count = len(running_jobs.data)
            except Exception as check_error:
                print(f"⚠️ Job monitor: Failed to check running jobs: {check_error}")
            
            if running_count == 0:
                # No running jobs, try to start next
                start_next_queued_job()
            else:
                # Jobs are running, just monitor
                pass
                
            time.sleep(10)  # Check every 10 seconds
        except KeyboardInterrupt:
            print("🛑 Job monitor shutting down...")
            break
        except Exception as e:
            print(f"❌ Job monitor error: {e}")
            traceback.print_exc()
            # Don't sleep too long on error to keep monitoring active
            time.sleep(10)

# ==================== API ENDPOINTS ====================

@app.get("/health")
async def health_check():
    return {
        "status": "healthy", 
        "timestamp": datetime.utcnow().isoformat(),
        "redis_connected": bool(get_redis_client()),
        "supabase_connected": bool(get_supabase_client()),
        "python_version": sys.version,
        "deployment_version": "v2.2-force-deploy"
    }

@app.post("/auth/login")
async def login(credentials: dict):
    username = credentials.get("username")
    password = credentials.get("password")
    
    # Simple hardcoded auth for now
    if username == "admin" and password == "buzzberry2024":
        token = jwt.encode({"sub": username}, SECRET_KEY, algorithm=ALGORITHM)
        return {"access_token": token, "token_type": "bearer"}
    
    raise HTTPException(status_code=401, detail="Invalid credentials")

@app.get("/stats")
async def get_stats(current_user: str = Depends(verify_token)):
    try:
        supabase = get_supabase_client()
        if not supabase:
            raise HTTPException(status_code=500, detail="Database connection failed")

        # Get total creators
        total_response = supabase.table("creatordata").select("id", count="exact").execute()
        total_creators = total_response.count

        # Get Instagram creators
        instagram_response = supabase.table("creatordata").select("id", count="exact").eq("platform", "instagram").execute()
        instagram_creators = instagram_response.count

        # Get TikTok creators
        tiktok_response = supabase.table("creatordata").select("id", count="exact").eq("platform", "tiktok").execute()
        tiktok_creators = tiktok_response.count

        return {
            "total_creators": total_creators,
            "instagram_creators": instagram_creators,
            "tiktok_creators": tiktok_creators,
            "redis_connected": bool(get_redis_client()),
            "supabase_connected": bool(get_supabase_client())
        }
    except Exception as e:
        print(f"Stats error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/jobs")
async def get_jobs(current_user: str = Depends(verify_token)):
    try:
        supabase = get_supabase_client()
        if not supabase:
            raise HTTPException(status_code=500, detail="Database connection failed")

        response = supabase.table("scraper_jobs").select("*").order("created_at", desc=True).execute()
        return response.data
    except Exception as e:
        print(f"Jobs error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/jobs/upload-csv")
async def upload_csv(
    file: UploadFile = File(...),
    current_user: str = Depends(verify_token)
):
    try:
        # Read CSV file
        contents = await file.read()
        df = pd.read_csv(io.StringIO(contents.decode('utf-8')))
        
        # Validate CSV structure - check for both possible column name formats
        required_columns = ['username', 'platform']
        alt_columns = ['Usernames', 'Platform']
        
        # Check if CSV has the expected columns (either format)
        if all(col in df.columns for col in required_columns):
            # Standard format - no changes needed
            pass
        elif all(col in df.columns for col in alt_columns):
            # Alternative format - rename columns
            df = df.rename(columns={'Usernames': 'username', 'Platform': 'platform'})
            print(f"✅ Renamed CSV columns: Usernames->username, Platform->platform")
        else:
            raise HTTPException(status_code=400, detail=f"CSV must contain columns: {required_columns} or {alt_columns}")
        
        # Clean and validate data
        df = df.dropna(subset=['username', 'platform'])
        df['platform'] = df['platform'].str.lower()
        valid_platforms = ['instagram', 'tiktok']
        df = df[df['platform'].isin(valid_platforms)]
        
        if len(df) == 0:
            raise HTTPException(status_code=400, detail="No valid creators found in CSV")
        
        # Create job
        job_id = str(uuid.uuid4())
        
        supabase = get_supabase_client()
        if not supabase:
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        # Prepare CSV data
        csv_data = df.to_dict('records')
        
        # Determine if there are running jobs
        running_jobs = check_running_jobs()
        initial_status = JobStatus.QUEUED if running_jobs else JobStatus.PENDING
        
        # Create job record FIRST (with CSV data included)
        job_data = {
            "id": job_id,
            "job_type": "new_creators",
            "status": initial_status,
            "total_items": len(df),
            "processed_items": 0,
            "created_at": datetime.utcnow().isoformat(),
            "updated_at": datetime.utcnow().isoformat(),
            "description": json.dumps(csv_data)  # Store CSV data directly in job record
        }
        
        supabase.table("scraper_jobs").insert(job_data).execute()
        print(f"✅ Job {job_id} created with CSV data in Supabase")
        
        # Also store in Redis for faster access
        redis_client = get_redis_client()
        if redis_client:
            try:
                redis_client.setex(f"csv_data:{job_id}", 86400, json.dumps(csv_data))  # 24 hour expiry
                print(f"✅ CSV data stored in Redis for job {job_id}")
            except Exception as redis_error:
                print(f"⚠️ Redis storage failed: {redis_error}")
        else:
            print(f"⚠️ Redis not available, CSV data only stored in Supabase")
        
        # Start next job if none running
        if not running_jobs:
            start_next_queued_job()
        
        return {"job_id": job_id, "status": initial_status, "total_creators": len(df)}
        
    except Exception as e:
        print(f"Upload error: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/jobs/{job_id}/remove")
async def remove_job(job_id: str, current_user: str = Depends(verify_token)):
    """Remove a job from the database (only for completed, failed, or cancelled jobs)"""
    try:
        supabase = get_supabase_client()
        if not supabase:
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        # First check if the job exists and get its status
        job_response = supabase.table("scraper_jobs").select("*").eq("id", job_id).execute()
        
        if not job_response.data:
            print(f"❌ Remove failed: Job {job_id} not found in database")
            raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
        
        job_status = job_response.data[0]["status"]
        
        # Only allow removal of non-running jobs
        if job_status in [JobStatus.RUNNING, JobStatus.PENDING, JobStatus.QUEUED]:
            raise HTTPException(
                status_code=400, 
                detail=f"Cannot remove {job_status} job. Only completed, failed, or cancelled jobs can be removed."
            )
        
        # Delete the job
        delete_response = supabase.table("scraper_jobs").delete().eq("id", job_id).execute()
        
        if not delete_response.data:
            raise HTTPException(status_code=404, detail="Job not found or already removed")
        
        # Also clean up any CSV data in Redis
        redis_client = get_redis_client()
        if redis_client:
            try:
                redis_client.delete(f"csv_data:{job_id}")
            except Exception as redis_error:
                print(f"⚠️ Failed to clean up Redis data for job {job_id}: {redis_error}")
        
        return {"message": f"Job {job_id} removed successfully"}
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Remove job error: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/jobs/{job_id}/cancel")
async def cancel_job(job_id: str, current_user: str = Depends(verify_token)):
    """Cancel a running or pending job"""
    try:
        supabase = get_supabase_client()
        if not supabase:
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        # Check if job exists
        job_response = supabase.table("scraper_jobs").select("*").eq("id", job_id).execute()
        
        if not job_response.data:
            print(f"❌ Cancel failed: Job {job_id} not found in database")
            raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
        
        job_status = job_response.data[0]["status"]
        
        # Only allow cancellation of running, pending, or queued jobs
        if job_status not in [JobStatus.RUNNING, JobStatus.PENDING, JobStatus.QUEUED]:
            raise HTTPException(
                status_code=400, 
                detail=f"Cannot cancel {job_status} job. Only running, pending, or queued jobs can be cancelled."
            )
        
        # Update job status to cancelled
        update_response = supabase.table("scraper_jobs").update({
            "status": JobStatus.CANCELLED,
            "updated_at": datetime.utcnow().isoformat()
        }).eq("id", job_id).execute()
        
        if not update_response.data:
            raise HTTPException(status_code=404, detail="Job not found")
        
        # Start next queued job if this was running
        if job_status == JobStatus.RUNNING:
            start_next_queued_job()
        
        return {"message": f"Job {job_id} cancelled successfully"}
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Cancel job error: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/jobs/{job_id}/force-cancel")
async def force_cancel_job(job_id: str, current_user: str = Depends(verify_token)):
    """Force cancel any job regardless of status"""
    try:
        supabase = get_supabase_client()
        if not supabase:
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        # Check if job exists
        job_response = supabase.table("scraper_jobs").select("*").eq("id", job_id).execute()
        
        if not job_response.data:
            print(f"❌ Force cancel failed: Job {job_id} not found in database")
            raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
        
        job = job_response.data[0]
        job_status = job["status"]
        
        print(f"🔨 Force cancelling job {job_id} (current status: {job_status})")
        
        # Update job status to cancelled regardless of current status
        update_response = supabase.table("scraper_jobs").update({
            "status": JobStatus.CANCELLED,
            "updated_at": datetime.utcnow().isoformat(),
            "error_message": "Force cancelled by user"
        }).eq("id", job_id).execute()
        
        if not update_response.data:
            raise HTTPException(status_code=404, detail="Job not found")
        
        # Clean up any checkpoints
        redis_client = get_redis_client()
        if redis_client:
            try:
                redis_client.delete(f"checkpoint:{job_id}")
                redis_client.delete(f"csv_data:{job_id}")
                print(f"🧹 Cleaned up Redis data for job {job_id}")
            except Exception as redis_error:
                print(f"⚠️ Redis cleanup failed: {redis_error}")
        
        # Start next queued job if this was running
        if job_status == JobStatus.RUNNING:
            start_next_queued_job()
        
        return {"message": f"Job {job_id} force cancelled successfully"}
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Force cancel job error: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/jobs/{job_id}/resume")
async def resume_job(job_id: str, current_user: str = Depends(verify_token)):
    """Resume a failed or cancelled job"""
    try:
        supabase = get_supabase_client()
        if not supabase:
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        # Check if job exists and get details
        job_response = supabase.table("scraper_jobs").select("*").eq("id", job_id).execute()
        
        if not job_response.data:
            raise HTTPException(status_code=404, detail="Job not found")
        
        job = job_response.data[0]
        job_status = job["status"]
        job_type = job["job_type"]
        
        # Only allow resuming of failed or cancelled jobs
        if job_status not in [JobStatus.FAILED, JobStatus.CANCELLED]:
            raise HTTPException(
                status_code=400, 
                detail=f"Cannot resume {job_status} job. Only failed or cancelled jobs can be resumed."
            )
        
        # Check if there are running jobs
        if check_running_jobs():
            # Set to queued if other jobs are running
            new_status = JobStatus.QUEUED
        else:
            new_status = JobStatus.PENDING
        
        # Update job status
        supabase.table("scraper_jobs").update({
            "status": new_status,
            "updated_at": datetime.utcnow().isoformat(),
            "error_message": None  # Clear any previous error
        }).eq("id", job_id).execute()
        
        # Start job if no others are running
        if new_status == JobStatus.PENDING:
            start_next_queued_job()
        
        return {"message": f"Job {job_id} resumed successfully", "status": new_status}
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Resume job error: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/jobs/rescrape")
async def create_rescrape_job(request: dict, current_user: str = Depends(verify_token)):
    """Create a new rescraping job"""
    try:
        job_type = request.get("type", "rescrape_all")  # Default to rescrape all
        platform = request.get("platform", "all")
        
        # Generate job ID
        job_id = str(uuid.uuid4())
        
        supabase = get_supabase_client()
        if not supabase:
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        # Determine job type and description based on request
        if job_type == "rescrape_all" or platform == "all":
            job_type_db = "rescrape_all"
            description = "Rescrape all creators (Instagram and TikTok)"
            # Get total count of all creators
            total_response = supabase.table("creatordata").select("id", count="exact").execute()
            total_items = total_response.count
        elif platform == "instagram":
            job_type_db = "rescrape_instagram"
            description = "Rescrape all Instagram creators"
            # Get count of Instagram creators
            total_response = supabase.table("creatordata").select("id", count="exact").eq("platform", "instagram").execute()
            total_items = total_response.count
        elif platform == "tiktok":
            job_type_db = "rescrape_tiktok"
            description = "Rescrape all TikTok creators"
            # Get count of TikTok creators
            total_response = supabase.table("creatordata").select("id", count="exact").eq("platform", "tiktok").execute()
            total_items = total_response.count
        else:
            raise HTTPException(status_code=400, detail="Invalid platform specified")
        
        # Check if there are running jobs
        running_jobs = check_running_jobs()
        initial_status = JobStatus.QUEUED if running_jobs else JobStatus.PENDING
        
        # Create job record
        job_data = {
            "id": job_id,
            "job_type": job_type_db,
            "status": initial_status,
            "total_items": total_items,
            "processed_items": 0,
            "created_at": datetime.utcnow().isoformat(),
            "updated_at": datetime.utcnow().isoformat(),
            "description": description
        }
        
        supabase.table("scraper_jobs").insert(job_data).execute()
        print(f"✅ Rescrape job {job_id} created ({job_type_db})")
        
        # Start job if no others are running
        if not running_jobs:
            start_next_queued_job()
        
        return {"job_id": job_id, "status": initial_status, "total_creators": total_items}
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Create rescrape job error: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/jobs/{job_id}/restart")
async def restart_stuck_job(job_id: str, current_user: str = Depends(verify_token)):
    """Restart a stuck job from its last checkpoint"""
    try:
        supabase = get_supabase_client()
        if not supabase:
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        # Check if job exists
        job_response = supabase.table("scraper_jobs").select("*").eq("id", job_id).execute()
        
        if not job_response.data:
            raise HTTPException(status_code=404, detail="Job not found")
        
        job = job_response.data[0]
        job_type = job["job_type"]
        
        # Only allow restarting of failed or stuck jobs
        if job["status"] not in ["failed", "running"]:
            raise HTTPException(
                status_code=400, 
                detail=f"Cannot restart {job['status']} job. Only failed or running jobs can be restarted."
            )
        
        # Check if there are other running jobs
        if check_running_jobs() and job["status"] != "running":
            new_status = JobStatus.QUEUED
        else:
            new_status = JobStatus.PENDING
        
        # Update job status
        supabase.table("scraper_jobs").update({
            "status": new_status,
            "updated_at": datetime.utcnow().isoformat(),
            "error_message": None  # Clear any previous error
        }).eq("id", job_id).execute()
        
        print(f"🔄 Restarting stuck job {job_id} from checkpoint")
        
        # Start job if no others are running
        if new_status == JobStatus.PENDING:
            start_next_queued_job()
        
        return {"message": f"Job {job_id} restarted successfully", "status": new_status}
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Restart job error: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/jobs/emergency-cleanup")
async def emergency_cleanup(current_user: str = Depends(verify_token)):
    """Emergency cleanup of all phantom/stuck jobs"""
    try:
        supabase = get_supabase_client()
        if not supabase:
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        # Get all running jobs
        running_jobs = supabase.table("scraper_jobs").select("*").eq("status", JobStatus.RUNNING).execute()
        
        cleanup_count = 0
        for job in running_jobs.data:
            job_id = job["id"]
            print(f"🧹 Emergency cleanup: Marking job {job_id} as failed")
            
            # Mark as failed
            supabase.table("scraper_jobs").update({
                "status": JobStatus.FAILED,
                "error_message": "Emergency cleanup - job was stuck",
                "updated_at": datetime.utcnow().isoformat()
            }).eq("id", job_id).execute()
            
            # Clean up Redis data
            redis_client = get_redis_client()
            if redis_client:
                try:
                    redis_client.delete(f"checkpoint:{job_id}")
                    redis_client.delete(f"csv_data:{job_id}")
                except Exception as redis_error:
                    print(f"⚠️ Redis cleanup failed for {job_id}: {redis_error}")
            
            cleanup_count += 1
        
        print(f"✅ Emergency cleanup complete: {cleanup_count} jobs cleaned up")
        return {"message": f"Emergency cleanup complete: {cleanup_count} jobs cleaned up"}
        
    except Exception as e:
        print(f"Emergency cleanup error: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

# ==================== RESCRAPING MANAGEMENT ====================

@app.get("/rescraping/stats")
async def get_rescraping_stats(current_user: str = Depends(verify_token)):
    """Get rescraping statistics and schedule"""
    try:
        supabase = get_supabase_client()
        if not supabase:
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        from datetime import datetime, timedelta
        
        # Get total creators
        total_response = supabase.table("creatordata").select("id", count="exact").execute()
        total_creators = total_response.count
        
        # Get creators with no updated_at (need initial dates)
        null_updated_response = supabase.table("creatordata").select("id", count="exact").is_("updated_at", "null").execute()
        creators_need_dates = null_updated_response.count
        
        # Get creators due for rescraping TODAY (updated exactly 7 days ago)
        today = datetime.utcnow().date()
        seven_days_ago_start = (today - timedelta(days=7))
        seven_days_ago_end = seven_days_ago_start + timedelta(days=1)
        
        # Count creators due today (updated 7 days ago)
        due_response = supabase.table("creatordata").select("id", count="exact").gte("updated_at", seven_days_ago_start.isoformat()).lt("updated_at", seven_days_ago_end.isoformat()).execute()
        creators_due = due_response.count + creators_need_dates  # Include creators with null dates
        
        # Set daily rescraping time to 8:00 AM San Francisco time (15:00 UTC in PDT)
        DAILY_RESCRAPE_HOUR = 8  # San Francisco time
        DAILY_RESCRAPE_UTC_HOUR = 15  # UTC equivalent (PDT)
        
        # Get creators by day of week for next 7 days based on actual updated_at dates
        schedule = {}
        for i in range(7):
            target_date = datetime.utcnow() + timedelta(days=i)
            day_name = target_date.strftime("%A")
            
            # Calculate the date 7 days ago from target_date (when creators would have been last updated)
            seven_days_before_target = target_date - timedelta(days=7)
            start_of_day = seven_days_before_target.replace(hour=0, minute=0, second=0, microsecond=0)
            end_of_day = start_of_day + timedelta(days=1)
            
            # Count creators that were updated on that day (and thus due for rescraping on target_date)
            creators_due_count = supabase.table("creatordata").select("id", count="exact").gte("updated_at", start_of_day.isoformat()).lt("updated_at", end_of_day.isoformat()).execute()
            
            # Add creators with null dates if this is today (they need immediate attention)
            if i == 0:
                creators_due_count.count += creators_need_dates
            
            # Set scheduled time for daily rescrape (8:00 AM San Francisco time = 15:00 UTC)
            scheduled_time_utc = target_date.replace(hour=DAILY_RESCRAPE_UTC_HOUR, minute=0, second=0, microsecond=0)
            
            schedule[day_name] = {
                "date": target_date.strftime("%Y-%m-%d"),
                "day": day_name,
                "estimated_creators": creators_due_count.count,
                "scheduled_time": f"{DAILY_RESCRAPE_HOUR}:00 AM PST/PDT",
                "scheduled_time_utc": scheduled_time_utc.strftime("%H:%M UTC"),
                "is_today": i == 0,
                "is_past_time": datetime.utcnow() > scheduled_time_utc if i == 0 else False
            }
        
        # Get recent rescraping jobs (including daily jobs)
        recent_jobs = supabase.table("scraper_jobs").select("*").in_("job_type", ["rescrape_all", "rescrape_instagram", "rescrape_tiktok", "daily_rescrape"]).order("created_at", desc=True).limit(10).execute()
        
        # Get additional breakdown for better UI
        # Count truly overdue creators (MORE than 7 days old - 8+ days)
        eight_days_ago = (datetime.utcnow() - timedelta(days=8)).isoformat()
        truly_overdue_response = supabase.table("creatordata").select("id", count="exact").lt("updated_at", eight_days_ago).execute()
        truly_overdue = truly_overdue_response.count
        
        # Count ALL creators 7+ days old (for daily rescraping)
        seven_days_ago_total = (datetime.utcnow() - timedelta(days=7)).isoformat()
        total_overdue_response = supabase.table("creatordata").select("id", count="exact").lt("updated_at", seven_days_ago_total).execute()
        total_overdue = total_overdue_response.count
        
        # Count just today's batch (exactly 7 days ago)
        todays_batch = due_response.count  # This is from the existing query above
        
        return {
            "total_creators": total_creators,
            "creators_need_dates": creators_need_dates,
            "creators_due_rescrape": creators_due,  # Today's batch + null dates
            "total_overdue_creators": total_overdue,  # ALL overdue (7+ days) - used by daily job
            "todays_scheduled_batch": todays_batch,  # Just today's exact batch
            "remaining_overdue": truly_overdue + creators_need_dates,  # Actually overdue (8+ days) + null dates
            "weekly_schedule": schedule,
            "recent_jobs": recent_jobs.data,
            "schedule_info": {
                "time_sf": f"{DAILY_RESCRAPE_HOUR}:00 AM PST/PDT",
                "time_utc": f"{DAILY_RESCRAPE_UTC_HOUR}:00 UTC",
                "description": "Automatic daily rescraping includes ALL overdue creators (not just today's batch)"
            }
        }
        
    except Exception as e:
        print(f"Rescraping stats error: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/rescraping/populate-dates")
async def populate_updated_dates(current_user: str = Depends(verify_token)):
    """Populate null updated_at dates spread across the past week"""
    try:
        supabase = get_supabase_client()
        if not supabase:
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        from datetime import datetime, timedelta
        import random
        
        # Get creators with null updated_at
        null_creators = supabase.table("creatordata").select("id").is_("updated_at", "null").execute()
        
        if not null_creators.data:
            return {"message": "No creators need date population", "updated_count": 0}
        
        print(f"📅 Populating dates for {len(null_creators.data)} creators")
        
        updated_count = 0
        base_date = datetime.utcnow() - timedelta(days=14)  # Start from 14 days ago for better spread
        
        # Batch update for better performance
        batch_size = 50
        for batch_start in range(0, len(null_creators.data), batch_size):
            batch_end = min(batch_start + batch_size, len(null_creators.data))
            batch = null_creators.data[batch_start:batch_end]
            
            for i, creator in enumerate(batch):
                # Spread creators across 14 days for better distribution
                global_index = batch_start + i
                days_offset = global_index % 14
                hours_offset = random.randint(0, 23)
                minutes_offset = random.randint(0, 59)
                seconds_offset = random.randint(0, 59)
                
                updated_date = base_date + timedelta(
                    days=days_offset,
                    hours=hours_offset,
                    minutes=minutes_offset,
                    seconds=seconds_offset
                )
                
                # Update the creator's updated_at date
                try:
                    supabase.table("creatordata").update({
                        "updated_at": updated_date.isoformat()
                    }).eq("id", creator["id"]).execute()
                    updated_count += 1
                except Exception as update_error:
                    print(f"⚠️ Failed to update creator {creator['id']}: {update_error}")
            
            # Progress update every batch
            print(f"📅 Updated {updated_count}/{len(null_creators.data)} creators")
        
        print(f"✅ Completed populating dates for {updated_count} creators")
        return {"message": f"Successfully populated dates for {updated_count} creators", "updated_count": updated_count}
        
    except Exception as e:
        print(f"Populate dates error: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/rescraping/force-populate-dates")
async def force_populate_dates(current_user: str = Depends(verify_token)):
    """Force populate dates for ALL creators to ensure even weekly distribution"""
    try:
        supabase = get_supabase_client()
        if not supabase:
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        from datetime import datetime, timedelta
        import random
        
        # Get ALL creators (not just null ones)
        all_creators = supabase.table("creatordata").select("id").execute()
        
        if not all_creators.data:
            return {"message": "No creators found", "updated_count": 0}
        
        print(f"📅 Force populating dates for {len(all_creators.data)} creators")
        
        updated_count = 0
        total_creators = len(all_creators.data)
        
        # Spread across exactly 7 days for weekly distribution
        # Start from 8 days ago to ensure they're all due for rescraping
        base_date = datetime.utcnow() - timedelta(days=8)
        
        print(f"📊 Base date for distribution: {base_date.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"📊 This means creators will be due starting: {(base_date + timedelta(days=7)).strftime('%Y-%m-%d')}")
        
        # Batch update for better performance
        batch_size = 50
        for batch_start in range(0, total_creators, batch_size):
            batch_end = min(batch_start + batch_size, total_creators)
            batch = all_creators.data[batch_start:batch_end]
            
            for i, creator in enumerate(batch):
                # Spread creators evenly across 7 days
                global_index = batch_start + i
                days_offset = global_index % 7  # 0-6 days
                
                # Add some randomness within each day to avoid clustering
                hours_offset = random.randint(0, 23)
                minutes_offset = random.randint(0, 59)
                seconds_offset = random.randint(0, 59)
                
                updated_date = base_date + timedelta(
                    days=days_offset,
                    hours=hours_offset,
                    minutes=minutes_offset,
                    seconds=seconds_offset
                )
                
                # Update the creator's updated_at date
                try:
                    supabase.table("creatordata").update({
                        "updated_at": updated_date.isoformat()
                    }).eq("id", creator["id"]).execute()
                    updated_count += 1
                except Exception as update_error:
                    print(f"⚠️ Failed to update creator {creator['id']}: {update_error}")
            
            # Progress update every batch
            print(f"📅 Updated {updated_count}/{total_creators} creators")
        
        # Calculate expected distribution
        creators_per_day = total_creators // 7
        remainder = total_creators % 7
        
        print(f"✅ Completed force populating dates for {updated_count} creators")
        print(f"📊 Expected distribution: ~{creators_per_day} creators per day ({remainder} days will have +1 extra)")
        
        return {
            "message": f"Successfully redistributed {updated_count} creators across 7 days", 
            "updated_count": updated_count,
            "expected_per_day": creators_per_day,
            "days_with_extra": remainder
        }
        
    except Exception as e:
        print(f"Force populate dates error: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/rescraping/test-distribution")
async def test_distribution(current_user: str = Depends(verify_token)):
    """Test the current distribution of creators by updated_at date"""
    try:
        supabase = get_supabase_client()
        if not supabase:
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        from datetime import datetime, timedelta
        
        # Check distribution for the past 14 days
        distribution = {}
        for i in range(14):
            target_date = datetime.utcnow() - timedelta(days=i)
            start_of_day = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
            end_of_day = start_of_day + timedelta(days=1)
            
            count_response = supabase.table("creatordata").select("id", count="exact").gte("updated_at", start_of_day.isoformat()).lt("updated_at", end_of_day.isoformat()).execute()
            
            distribution[f"Day {i} ({target_date.strftime('%Y-%m-%d')})"] = count_response.count
        
        # Also check how many will be due each day for the next 7 days
        due_schedule = {}
        for i in range(7):
            future_date = datetime.utcnow() + timedelta(days=i)
            
            # Creators updated 7 days before this future date will be due
            seven_days_before = future_date - timedelta(days=7)
            start_of_day = seven_days_before.replace(hour=0, minute=0, second=0, microsecond=0)
            end_of_day = start_of_day + timedelta(days=1)
            
            count_response = supabase.table("creatordata").select("id", count="exact").gte("updated_at", start_of_day.isoformat()).lt("updated_at", end_of_day.isoformat()).execute()
            
            due_schedule[f"{future_date.strftime('%A')} ({future_date.strftime('%Y-%m-%d')})"] = count_response.count
        
        # Also get total counts for verification
        total_creators = supabase.table("creatordata").select("id", count="exact").execute().count
        null_creators = supabase.table("creatordata").select("id", count="exact").is_("updated_at", "null").execute().count
        
        return {
            "total_creators": total_creators,
            "null_creators": null_creators,
            "current_distribution_past_14_days": distribution,
            "creators_due_next_7_days": due_schedule,
            "explanation": "This shows how creators are currently distributed and when they'll be due for rescraping",
            "summary": {
                "total_due_next_7_days": sum(due_schedule.values()),
                "most_busy_day": max(due_schedule.items(), key=lambda x: x[1]) if due_schedule else None,
                "distribution_looks_even": max(due_schedule.values()) - min(due_schedule.values()) < 50 if due_schedule else False
            }
        }
        
    except Exception as e:
        print(f"Test distribution error: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/rescraping/fix-distribution")
async def fix_rescraping_distribution(current_user: str = Depends(verify_token)):
    """Fix the rescraping distribution by evenly spreading ALL creators across 7 days."""
    try:
        print("🔧 Starting complete rescraping distribution fix...")
        
        # Get ALL creators (not just null ones)
        all_creators = supabase.table("creatordata").select("id, handle, platform").execute()
        
        if not all_creators.data:
            return {"message": "No creators found", "updated_count": 0}
        
        print(f"📅 Redistributing ALL {len(all_creators.data)} creators across 7 days")
        
        updated_count = 0
        total_creators = len(all_creators.data)
        
        # Spread creators across the PAST 7 days so they become due on DIFFERENT days
        # If a creator was updated 7 days ago, they're due TODAY
        # If a creator was updated 6 days ago, they're due TOMORROW, etc.
        
        now = datetime.utcnow()
        print(f"📊 Current time: {now.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"📊 Spreading creators across past 7 days for staggered due dates")
        
        # Batch update for better performance
        batch_size = 50
        for batch_start in range(0, total_creators, batch_size):
            batch_end = min(batch_start + batch_size, total_creators)
            batch_creators = all_creators.data[batch_start:batch_end]
            
            for i, creator in enumerate(batch_creators):
                global_index = batch_start + i
                
                # Calculate which day this creator should be assigned to (0-6)
                day_offset = global_index % 7
                
                # Calculate the target date: spread across PAST 7 days from today (Aug 25th)
                # day_offset=0 -> 7 days ago (Aug 18th, due today Aug 25th)
                # day_offset=1 -> 6 days ago (Aug 19th, due tomorrow Aug 26th)
                # day_offset=6 -> 1 day ago (Aug 24th, due Aug 31st)
                days_ago = 7 - day_offset
                target_date = now - timedelta(days=days_ago)
                
                # Add random time within the day to avoid clustering
                random_hours = random.randint(6, 18)  # Between 6 AM and 6 PM
                random_minutes = random.randint(0, 59)
                random_seconds = random.randint(0, 59)
                
                # Set the specific date with random time
                target_date = target_date.replace(
                    hour=random_hours, 
                    minute=random_minutes, 
                    second=random_seconds, 
                    microsecond=random.randint(0, 999999)
                )
                
                # Show when this creator will be due (7 days after updated_at)
                due_date = target_date + timedelta(days=7)
                if global_index < 10:  # Show first 10 for debugging
                    print(f"📅 Creator {global_index}: updated_at={target_date.strftime('%Y-%m-%d')}, due={due_date.strftime('%Y-%m-%d')}")
                
                try:
                    supabase.table("creatordata").update({
                        "updated_at": target_date.isoformat()
                    }).eq("id", creator["id"]).execute()
                    
                    updated_count += 1
                    
                except Exception as e:
                    print(f"❌ Error updating creator {creator['handle']}: {e}")
                    continue
            
            print(f"📊 Processed batch {batch_start}-{batch_end} ({updated_count}/{total_creators} updated)")
        
        print(f"✅ Successfully redistributed {updated_count} creators across 7 days")
        
        return {
            "message": f"Successfully redistributed {updated_count} creators across past 7 days for staggered due dates",
            "updated_count": updated_count,
            "total_creators": total_creators,
            "current_time": now.strftime('%Y-%m-%d %H:%M:%S'),
            "explanation": "Creators spread across past 7 days so they become due on different days (7 days after their updated_at date)"
        }
        
    except Exception as e:
        print(f"Fix distribution error: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/rescraping/due-creators")
async def get_due_creators(current_user: str = Depends(verify_token)):
    """Get creators due for rescraping (>7 days old)"""
    try:
        supabase = get_supabase_client()
        if not supabase:
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        from datetime import datetime, timedelta
        
        seven_days_ago = (datetime.utcnow() - timedelta(days=7)).isoformat()
        
        # Get creators due for rescraping
        due_creators = supabase.table("creatordata").select("id", "handle", "platform", "updated_at", "primary_niche").lt("updated_at", seven_days_ago).order("updated_at").limit(100).execute()
        
        return {
            "creators_due": due_creators.data,
            "total_due": len(due_creators.data)
        }
        
    except Exception as e:
        print(f"Due creators error: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/rescraping/start-overdue-only")
async def start_overdue_rescrape(request: dict, current_user: str = Depends(verify_token)):
    """Start rescraping for only the overdue creators (selective re-run)"""
    try:
        platform = request.get("platform", "all")  # all, instagram, tiktok
        max_creators = request.get("max_creators", 200)  # Higher limit for cleanup runs
        
        supabase = get_supabase_client()
        if not supabase:
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        from datetime import datetime, timedelta
        
        # Use 8+ days for truly overdue creators (not just due today)
        eight_days_ago = (datetime.utcnow() - timedelta(days=8)).isoformat()
        
        # Build query based on platform
        query = supabase.table("creatordata").select("id", "handle", "platform", "updated_at")
        
        if platform == "instagram":
            query = query.eq("platform", "instagram")
        elif platform == "tiktok":
            query = query.eq("platform", "tiktok")
        
        # Get truly overdue creators (8+ days old) + those with null dates
        overdue_creators = query.lt("updated_at", eight_days_ago).order("updated_at").limit(max_creators).execute()
        null_creators = supabase.table("creatordata").select("id", "handle", "platform").is_("updated_at", "null").limit(50).execute()
        
        all_creators = overdue_creators.data + null_creators.data
        
        if not all_creators:
            return {"message": "No overdue creators found for rescraping", "job_id": None}
        
        # Create rescraping job
        job_id = str(uuid.uuid4())
        job_type_map = {
            "all": "rescrape_overdue_all",
            "instagram": "rescrape_overdue_instagram", 
            "tiktok": "rescrape_overdue_tiktok"
        }
        
        # Check if there are running jobs
        running_jobs = check_running_jobs()
        initial_status = JobStatus.QUEUED if running_jobs else JobStatus.PENDING
        
        job_data = {
            "id": job_id,
            "job_type": job_type_map[platform],
            "status": initial_status,
            "total_items": len(all_creators),
            "processed_items": 0,
            "created_at": datetime.utcnow().isoformat(),
            "updated_at": datetime.utcnow().isoformat(),
            "description": f"Overdue cleanup: {len(overdue_creators.data)} truly overdue (8+ days) + {len(null_creators.data)} null dates ({platform})"
        }
        
        supabase.table("scraper_jobs").insert(job_data).execute()
        
        # Store creator list in Redis
        redis_client = get_redis_client()
        redis_key = f"rescrape_job:{job_id}"
        
        creator_data = {
            "creators": all_creators,
            "platform_filter": platform,
            "job_type": job_data["job_type"]
        }
        
        redis_client.setex(redis_key, 86400, json.dumps(creator_data))  # 24 hours TTL
        
        return {
            "message": f"Started overdue rescraping job for {len(all_creators)} creators",
            "job_id": job_id,
            "total_creators": len(all_creators),
            "overdue_creators": len(overdue_creators.data),
            "null_date_creators": len(null_creators.data),
            "platform": platform,
            "status": initial_status
        }
        
    except Exception as e:
        print(f"Overdue rescrape error: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/rescraping/start-todays-batch")
async def start_todays_batch_rescrape(request: dict, current_user: str = Depends(verify_token)):
    """Start rescraping for today's missed batch (exactly 7 days old creators)"""
    try:
        platform = request.get("platform", "all")  # all, instagram, tiktok
        
        supabase = get_supabase_client()
        if not supabase:
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        from datetime import datetime, timedelta
        
        # Get creators updated exactly 7 days ago (today's scheduled batch)
        seven_days_ago_start = (datetime.utcnow() - timedelta(days=7)).replace(hour=0, minute=0, second=0, microsecond=0)
        seven_days_ago_end = seven_days_ago_start + timedelta(days=1)
        
        # Build query based on platform
        query = supabase.table("creatordata").select("id", "handle", "platform", "updated_at")
        
        if platform == "instagram":
            query = query.eq("platform", "instagram")
        elif platform == "tiktok":
            query = query.eq("platform", "tiktok")
        
        # Get today's batch (exactly 7 days old) + any null dates
        todays_batch = query.gte("updated_at", seven_days_ago_start.isoformat()).lt("updated_at", seven_days_ago_end.isoformat()).order("updated_at").execute()
        null_creators = supabase.table("creatordata").select("id", "handle", "platform").is_("updated_at", "null").limit(50).execute()
        
        all_creators = todays_batch.data + null_creators.data
        
        if not all_creators:
            return {"message": "No creators from today's batch found", "job_id": None}
        
        # Create rescraping job
        job_id = str(uuid.uuid4())
        job_type_map = {
            "all": "rescrape_todays_batch_all",
            "instagram": "rescrape_todays_batch_instagram", 
            "tiktok": "rescrape_todays_batch_tiktok"
        }
        
        # Check if there are running jobs
        running_jobs = check_running_jobs()
        initial_status = JobStatus.QUEUED if running_jobs else JobStatus.PENDING
        
        job_data = {
            "id": job_id,
            "job_type": job_type_map[platform],
            "status": initial_status,
            "total_items": len(all_creators),
            "processed_items": 0,
            "created_at": datetime.utcnow().isoformat(),
            "updated_at": datetime.utcnow().isoformat(),
            "description": f"Today's missed batch: {len(todays_batch.data)} due today + {len(null_creators.data)} null dates ({platform})"
        }
        
        supabase.table("scraper_jobs").insert(job_data).execute()
        
        # Store creator list in Redis
        redis_client = get_redis_client()
        redis_key = f"rescrape_job:{job_id}"
        
        creator_data = {
            "creators": all_creators,
            "platform_filter": platform,
            "job_type": job_data["job_type"]
        }
        
        redis_client.setex(redis_key, 86400, json.dumps(creator_data))  # 24 hours TTL
        
        return {
            "message": f"Started today's batch rescraping for {len(all_creators)} creators",
            "job_id": job_id,
            "total_creators": len(all_creators),
            "todays_batch": len(todays_batch.data),
            "null_date_creators": len(null_creators.data),
            "platform": platform,
            "status": initial_status
        }
        
    except Exception as e:
        print(f"Today's batch rescrape error: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/rescraping/corrupted-creators")
async def get_corrupted_creators(current_user: str = Depends(verify_token)):
    """Get creators with missing niches or corrupted data from the rescraper bug"""
    try:
        supabase = get_supabase_client()
        if not supabase:
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        # Find creators with missing primary_niche OR secondary_niche
        missing_niches = supabase.table("creatordata").select("id", "handle", "platform", "primary_niche", "secondary_niche", "updated_at", "followers_count", "average_views", "engagement_rate").or_("primary_niche.is.null,secondary_niche.is.null").order("updated_at", desc=True).limit(100).execute()
        
        # Also find creators with suspicious zero metrics (likely corrupted)
        zero_metrics = supabase.table("creatordata").select("id", "handle", "platform", "primary_niche", "secondary_niche", "updated_at", "followers_count", "average_views", "engagement_rate").or_("average_views.eq.0,engagement_rate.eq.0").gt("followers_count", 1000).order("updated_at", desc=True).limit(50).execute()
        
        # Combine and deduplicate by handle
        all_corrupted = {}
        
        for creator in missing_niches.data:
            all_corrupted[creator['handle']] = {
                **creator,
                'issues': []
            }
            if not creator.get('primary_niche'):
                all_corrupted[creator['handle']]['issues'].append('missing_primary_niche')
            if not creator.get('secondary_niche'):
                all_corrupted[creator['handle']]['issues'].append('missing_secondary_niche')
        
        for creator in zero_metrics.data:
            if creator['handle'] not in all_corrupted:
                all_corrupted[creator['handle']] = {
                    **creator,
                    'issues': []
                }
            
            if creator.get('average_views', 0) == 0:
                all_corrupted[creator['handle']]['issues'].append('zero_average_views')
            if creator.get('engagement_rate', 0) == 0:
                all_corrupted[creator['handle']]['issues'].append('zero_engagement_rate')
        
        corrupted_list = list(all_corrupted.values())
        
        return {
            "corrupted_creators": corrupted_list,
            "total_count": len(corrupted_list),
            "missing_niches_count": len(missing_niches.data),
            "zero_metrics_count": len(zero_metrics.data)
        }
        
    except Exception as e:
        print(f"Corrupted creators error: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/rescraping/fix-corrupted-creators")
async def fix_corrupted_creators(request: dict, current_user: str = Depends(verify_token)):
    """Start rescraping job to fix creators with missing niches or corrupted data"""
    try:
        platform = request.get("platform", "all")  # all, instagram, tiktok
        max_creators = request.get("max_creators", 100)  # Limit per job
        
        supabase = get_supabase_client()
        if not supabase:
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        # Get corrupted creators (missing niches OR zero metrics)
        missing_niches_query = supabase.table("creatordata").select("id", "handle", "platform")
        zero_metrics_query = supabase.table("creatordata").select("id", "handle", "platform")
        
        if platform == "instagram":
            missing_niches_query = missing_niches_query.eq("platform", "instagram")
            zero_metrics_query = zero_metrics_query.eq("platform", "instagram")
        elif platform == "tiktok":
            missing_niches_query = missing_niches_query.eq("platform", "tiktok")
            zero_metrics_query = zero_metrics_query.eq("platform", "tiktok")
        
        # Find creators with missing niches
        missing_niches = missing_niches_query.or_("primary_niche.is.null,secondary_niche.is.null").limit(max_creators // 2).execute()
        
        # Find creators with zero metrics but decent follower count (likely corrupted)
        zero_metrics = zero_metrics_query.or_("average_views.eq.0,engagement_rate.eq.0").gt("followers_count", 1000).limit(max_creators // 2).execute()
        
        # Combine and deduplicate
        all_creators = {}
        for creator in missing_niches.data:
            all_creators[creator['handle']] = creator
        for creator in zero_metrics.data:
            if creator['handle'] not in all_creators:
                all_creators[creator['handle']] = creator
        
        creators_to_fix = list(all_creators.values())
        
        if not creators_to_fix:
            return {"message": "No corrupted creators found to fix", "job_id": None}
        
        # Create rescraping job
        job_id = str(uuid.uuid4())
        job_type_map = {
            "all": "fix_corrupted_all",
            "instagram": "fix_corrupted_instagram", 
            "tiktok": "fix_corrupted_tiktok"
        }
        
        # Check if there are running jobs
        running_jobs = check_running_jobs()
        initial_status = JobStatus.QUEUED if running_jobs else JobStatus.PENDING
        
        job_data = {
            "id": job_id,
            "job_type": job_type_map[platform],
            "status": initial_status,
            "total_items": len(creators_to_fix),
            "processed_items": 0,
            "created_at": datetime.utcnow().isoformat(),
            "updated_at": datetime.utcnow().isoformat(),
            "description": f"Fix corrupted data: {len(missing_niches.data)} missing niches + {len(zero_metrics.data)} zero metrics ({platform})"
        }
        
        supabase.table("scraper_jobs").insert(job_data).execute()
        
        # Store creator list in Redis
        redis_client = get_redis_client()
        redis_key = f"rescrape_job:{job_id}"
        
        creator_data = {
            "creators": creators_to_fix,
            "platform_filter": platform,
            "job_type": job_data["job_type"]
        }
        
        redis_client.setex(redis_key, 86400, json.dumps(creator_data))  # 24 hours TTL
        
        return {
            "message": f"Started corruption fix job for {len(creators_to_fix)} creators",
            "job_id": job_id,
            "total_creators": len(creators_to_fix),
            "missing_niches": len(missing_niches.data),
            "zero_metrics": len(zero_metrics.data),
            "platform": platform,
            "status": initial_status
        }
        
    except Exception as e:
        print(f"Fix corrupted creators error: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/rescraping/start-auto-rescrape")
async def start_auto_rescrape(request: dict, current_user: str = Depends(verify_token)):
    """Start automatic rescraping of due creators"""
    try:
        platform = request.get("platform", "all")  # all, instagram, tiktok
        max_creators = request.get("max_creators", 100)  # Limit per job
        
        supabase = get_supabase_client()
        if not supabase:
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        from datetime import datetime, timedelta
        
        seven_days_ago = (datetime.utcnow() - timedelta(days=7)).isoformat()
        
        # Build query based on platform
        query = supabase.table("creatordata").select("id", "handle", "platform")
        
        if platform == "instagram":
            query = query.eq("platform", "instagram")
        elif platform == "tiktok":
            query = query.eq("platform", "tiktok")
        # For "all", no additional filter needed
        
        due_creators = query.lt("updated_at", seven_days_ago).order("updated_at").limit(max_creators).execute()
        
        if not due_creators.data:
            return {"message": "No creators due for rescraping", "job_id": None}
        
        # Create rescraping job
        job_id = str(uuid.uuid4())
        job_type_map = {
            "all": "rescrape_all",
            "instagram": "rescrape_instagram", 
            "tiktok": "rescrape_tiktok"
        }
        
        # Check if there are running jobs
        running_jobs = check_running_jobs()
        initial_status = JobStatus.QUEUED if running_jobs else JobStatus.PENDING
        
        job_data = {
            "id": job_id,
            "job_type": job_type_map[platform],
            "status": initial_status,
            "total_items": len(due_creators.data),
            "processed_items": 0,
            "created_at": datetime.utcnow().isoformat(),
            "updated_at": datetime.utcnow().isoformat(),
            "description": f"Auto-rescrape {len(due_creators.data)} {platform} creators (7+ days old)"
        }
        
        supabase.table("scraper_jobs").insert(job_data).execute()
        print(f"✅ Auto-rescrape job {job_id} created for {len(due_creators.data)} creators")
        
        # Store creator list in Redis
        redis_client = get_redis_client()
        if redis_client:
            creator_data = [{"id": c["id"], "handle": c["handle"], "platform": c["platform"]} for c in due_creators.data]
            redis_client.setex(f"rescrape_data:{job_id}", 86400, json.dumps(creator_data))
        
        # Start job if no others are running
        if not running_jobs:
            start_next_queued_job()
        
        return {
            "message": f"Auto-rescrape job started for {len(due_creators.data)} creators",
            "job_id": job_id,
            "status": initial_status,
            "creators_count": len(due_creators.data)
        }
        
    except Exception as e:
        print(f"Start auto-rescrape error: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/rescraping/schedule-daily")
async def schedule_daily_rescraping(current_user: str = Depends(verify_token)):
    """Schedule a daily rescraping job for creators due today"""
    try:
        # Use the same logic as the automatic daily scheduler
        create_daily_rescraping_job()
        
        # Return status
        supabase = get_supabase_client()
        if not supabase:
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        from datetime import datetime
        today_str = datetime.utcnow().strftime('%Y-%m-%d')
        today_job = supabase.table("scraper_jobs").select("*").eq("job_type", "daily_rescrape").gte("created_at", f"{today_str}T00:00:00").lt("created_at", f"{today_str}T23:59:59").execute()
        
        if today_job.data:
            job = today_job.data[0]
            return {
                "message": f"Daily rescrape job created for {job['total_items']} creators",
                "job_id": job["id"],
                "status": job["status"],
                "creators_count": job["total_items"],
                "due_date": today_str
            }
        else:
            return {
                "message": "No creators due for rescraping today",
                "job_id": None,
                "creators_count": 0,
                "due_date": today_str
            }
        
    except Exception as e:
        print(f"Schedule daily rescrape error: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/rescraping/debug")
async def debug_rescraping_data(current_user: str = Depends(verify_token)):
    """Debug endpoint to check current state of creator updated_at dates"""
    try:
        supabase = get_supabase_client()
        if not supabase:
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        from datetime import datetime, timedelta
        
        # Get total creators
        total_response = supabase.table("creatordata").select("id", count="exact").execute()
        total_creators = total_response.count
        
        # Get creators with null updated_at
        null_response = supabase.table("creatordata").select("id", count="exact").is_("updated_at", "null").execute()
        null_count = null_response.count
        
        # Get creators older than 7 days
        seven_days_ago = (datetime.utcnow() - timedelta(days=7)).isoformat()
        old_response = supabase.table("creatordata").select("id", count="exact").lt("updated_at", seven_days_ago).execute()
        old_count = old_response.count
        
        # Get sample of recent updated_at dates
        recent_sample = supabase.table("creatordata").select("handle", "platform", "updated_at").is_not("updated_at", "null").order("updated_at", desc=False).limit(10).execute()
        
        # Get distribution by day for the past 14 days
        daily_distribution = {}
        for i in range(14):
            target_date = datetime.utcnow() - timedelta(days=i)
            start_of_day = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
            end_of_day = start_of_day + timedelta(days=1)
            
            count_response = supabase.table("creatordata").select("id", count="exact").gte("updated_at", start_of_day.isoformat()).lt("updated_at", end_of_day.isoformat()).execute()
            daily_distribution[target_date.strftime('%Y-%m-%d')] = count_response.count
        
        return {
            "total_creators": total_creators,
            "null_updated_at": null_count,
            "older_than_7_days": old_count,
            "recent_sample": recent_sample.data if recent_sample.data else [],
            "daily_distribution_past_14_days": daily_distribution,
            "debug_info": {
                "current_time": datetime.utcnow().isoformat(),
                "seven_days_ago_threshold": seven_days_ago,
                "note": "✅ Database trigger correctly updates updated_at when creators are rescraped"
            }
        }
        
    except Exception as e:
        print(f"Debug rescraping error: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/rescraping/daily-stats")
async def get_daily_rescraping_stats(current_user: str = Depends(verify_token)):
    """Get statistics for daily rescraping schedule"""
    try:
        supabase = get_supabase_client()
        if not supabase:
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        from datetime import datetime, timedelta
        
        # Get daily breakdown for next 7 days
        daily_stats = []
        for i in range(7):
            target_date = datetime.utcnow() - timedelta(days=7-i)
            start_date = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
            end_date = start_date + timedelta(days=1)
            
            # Count creators that will be due on this day
            due_count = supabase.table("creatordata").select("id", count="exact").gte("updated_at", start_date.isoformat()).lt("updated_at", end_date.isoformat()).execute()
            
            # Count by platform
            instagram_count = supabase.table("creatordata").select("id", count="exact").eq("platform", "instagram").gte("updated_at", start_date.isoformat()).lt("updated_at", end_date.isoformat()).execute()
            tiktok_count = supabase.table("creatordata").select("id", count="exact").eq("platform", "tiktok").gte("updated_at", start_date.isoformat()).lt("updated_at", end_date.isoformat()).execute()
            
            daily_stats.append({
                "date": (datetime.utcnow() + timedelta(days=i)).strftime('%Y-%m-%d'),
                "day_name": (datetime.utcnow() + timedelta(days=i)).strftime('%A'),
                "creators_due": due_count.count,
                "instagram_count": instagram_count.count,
                "tiktok_count": tiktok_count.count,
                "is_today": i == 0
            })
        
        return {"daily_schedule": daily_stats}
        
    except Exception as e:
        print(f"Daily stats error: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

# ==================== STARTUP EVENT ====================

@app.on_event("startup")
async def startup_event():
    try:
        print("🚀 Scraper Dashboard API starting up...")
        print(f"📊 Supabase URL: {SUPABASE_URL}")
        print(f"🔗 Redis URL: {REDIS_URL}")
        print(f"🌐 Port: {os.getenv('PORT', '8000')}")
        
        # Test connections
        supabase_ok = get_supabase_client() is not None
        redis_ok = get_redis_client() is not None
        
        print(f"📊 Supabase connection: {'✅' if supabase_ok else '❌'}")
        print(f"🔗 Redis connection: {'✅' if redis_ok else '❌'}")
        
        # CRITICAL: Clean up orphaned jobs on startup
        try:
            print("🧹 Cleaning up orphaned jobs from previous sessions...")
            supabase = get_supabase_client()
            if supabase:
                # Mark all "running" jobs as failed since they can't survive restarts
                orphaned_jobs = supabase.table("scraper_jobs").select("id", "job_type").eq("status", JobStatus.RUNNING).execute()
                
                if orphaned_jobs.data:
                    print(f"🧹 Found {len(orphaned_jobs.data)} orphaned running jobs")
                    for job in orphaned_jobs.data:
                        job_id = job["id"]
                        print(f"🧹 Marking orphaned job {job_id} as failed")
                        supabase.table("scraper_jobs").update({
                            "status": JobStatus.FAILED,
                            "error_message": "Service restarted - job orphaned",
                            "updated_at": datetime.utcnow().isoformat()
                        }).eq("id", job_id).execute()
                        
                        # Clean up Redis data
                        redis_client = get_redis_client()
                        if redis_client:
                            try:
                                redis_client.delete(f"checkpoint:{job_id}")
                                redis_client.delete(f"csv_data:{job_id}")
                            except:
                                pass
                    
                    print(f"✅ Cleaned up {len(orphaned_jobs.data)} orphaned jobs")
                else:
                    print("✅ No orphaned jobs found")
        except Exception as cleanup_error:
            print(f"⚠️ Startup cleanup failed: {cleanup_error}")
        
        # Initialize simple components
        try:
            print("🔧 Initializing simple scraper system...")
            
            # Test scraper initialization
            scraper = get_scraper()
            if scraper:
                print("✅ Simple scraper initialized")
            else:
                print("❌ Simple scraper initialization failed")
            
            # Start daily scheduler (simplified)
            try:
                scheduler_thread = threading.Thread(target=run_daily_scheduler, daemon=True)
                scheduler_thread.start()
                print("✅ Daily scheduler started")
            except Exception as scheduler_error:
                print(f"⚠️ Daily scheduler failed to start: {scheduler_error}")
            
            print("🚀 Simple system initialization complete!")
            
        except Exception as system_error:
            print(f"❌ Simple system initialization failed: {system_error}")
            traceback.print_exc()
            # Don't fail startup if system initialization fails
        
        print("✅ Scraper Dashboard API started")
        
    except Exception as e:
        print(f"❌ Startup error: {e}")
        traceback.print_exc()
        # Don't raise the exception to prevent shutdown

@app.on_event("shutdown")
async def shutdown_event():
    print("🛑 Scraper Dashboard API shutting down...")
    print("💾 Application shutdown complete")

# ==================== SIGNAL HANDLERS ====================

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    print(f"🛑 Received signal {signum}, shutting down gracefully...")
    sys.exit(0)

# Register signal handlers
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def create_daily_rescraping_job():
    """Create a daily rescraping job for ALL overdue creators (not just today's batch)"""
    try:
        print("⏰ Checking for daily rescraping job...")
        supabase = get_supabase_client()
        if not supabase:
            print("❌ Database connection failed for daily job creation")
            return
            
        from datetime import datetime, timedelta
        
        # Get ALL creators that are overdue (7+ days old) - this fixes the "remaining creators" issue
        seven_days_ago = (datetime.utcnow() - timedelta(days=7)).isoformat()
        
        # Find ALL creators due for rescraping (7+ days old) + creators with null dates
        overdue_creators = supabase.table("creatordata").select("id", "handle", "platform").lt("updated_at", seven_days_ago).order("updated_at").execute()
        null_creators = supabase.table("creatordata").select("id", "handle", "platform").is_("updated_at", "null").execute()
        
        all_due_creators = overdue_creators.data + null_creators.data
        print(f"📊 Found {len(overdue_creators.data)} overdue creators + {len(null_creators.data)} with null dates = {len(all_due_creators)} total")
        
        if not all_due_creators:
            print("✅ No creators due for rescraping today")
            return
        
        # Check if a daily job already exists for today
        today_str = datetime.utcnow().strftime('%Y-%m-%d')
        existing_job = supabase.table("scraper_jobs").select("id").eq("job_type", "daily_rescrape").gte("created_at", f"{today_str}T00:00:00").lt("created_at", f"{today_str}T23:59:59").execute()
        
        if existing_job.data:
            print(f"✅ Daily rescraping job already exists for {today_str}")
            return
            
        # Create daily rescraping job
        job_id = str(uuid.uuid4())
        
        # Check if there are running jobs
        running_jobs = check_running_jobs()
        initial_status = JobStatus.QUEUED if running_jobs else JobStatus.PENDING
        
        job_data = {
            "id": job_id,
            "job_type": "daily_rescrape",
            "status": initial_status,
            "total_items": len(all_due_creators),
            "processed_items": 0,
            "created_at": datetime.utcnow().isoformat(),
            "updated_at": datetime.utcnow().isoformat(),
            "description": f"Daily rescrape - {len(overdue_creators.data)} overdue + {len(null_creators.data)} null dates = {len(all_due_creators)} total creators ({today_str})"
        }
        
        supabase.table("scraper_jobs").insert(job_data).execute()
        print(f"✅ Created daily rescraping job {job_id} for {len(all_due_creators)} creators")
        
        # Store creator list in Redis
        redis_client = get_redis_client()
        if redis_client:
            creator_data = [{"id": c["id"], "handle": c["handle"], "platform": c["platform"]} for c in all_due_creators]
            redis_client.setex(f"rescrape_data:{job_id}", 86400, json.dumps(creator_data))
        
        # Start job if no others are running
        if not running_jobs:
            start_next_queued_job()
            
    except Exception as e:
        print(f"❌ Failed to create daily rescraping job: {e}")
        traceback.print_exc()

def run_daily_scheduler():
    """Run a timezone-aware daily scheduler for 8:00 AM San Francisco time"""
    print("📅 Starting daily job scheduler (8:00 AM San Francisco time)...")
    
    while True:
        try:
            # Calculate San Francisco time (PDT = UTC-7, PST = UTC-8)
            # For simplicity, assuming PDT (most of the year): 8 AM PDT = 15:00 UTC
            now = datetime.utcnow()
            
            # Check if it's 15:00 UTC (8:00 AM PDT) 
            if now.hour == 15 and now.minute == 0:
                print(f"⏰ Triggering daily rescrape at {now.strftime('%Y-%m-%d %H:%M')} UTC (8:00 AM PDT)")
                create_daily_rescraping_job()
                # Sleep for 2 minutes to avoid creating multiple jobs in the same minute
                time.sleep(120)
            else:
                # Check every minute
                time.sleep(60)
        except KeyboardInterrupt:
            print("🛑 Daily scheduler shutting down...")
            break
        except Exception as e:
            print(f"❌ Daily scheduler error: {e}")
            time.sleep(60)

@app.post("/jobs/emergency-restart-monitor")
async def emergency_restart_monitor(current_user: str = Depends(verify_token)):
    """Emergency endpoint to clean up orphaned jobs and restart the job monitor"""
    try:
        supabase = get_supabase_client()
        if not supabase:
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        # Find all orphaned running jobs
        orphaned_jobs = supabase.table("scraper_jobs").select("id", "job_type", "description").eq("status", JobStatus.RUNNING).execute()
        
        cleanup_results = []
        
        if orphaned_jobs.data:
            print(f"🧹 Emergency cleanup: Found {len(orphaned_jobs.data)} orphaned jobs")
            
            for job in orphaned_jobs.data:
                job_id = job["id"]
                job_desc = job.get("description", f"Job {job_id}")
                
                # Mark as failed
                supabase.table("scraper_jobs").update({
                    "status": JobStatus.FAILED,
                    "error_message": "Emergency cleanup - job was orphaned (server crashed)",
                    "updated_at": datetime.utcnow().isoformat()
                }).eq("id", job_id).execute()
                
                # Clean up Redis data
                redis_client = get_redis_client()
                if redis_client:
                    try:
                        redis_client.delete(f"checkpoint:{job_id}")
                        redis_client.delete(f"rescrape_job:{job_id}")
                        redis_client.delete(f"csv_data:{job_id}")
                    except Exception as redis_error:
                        print(f"⚠️ Redis cleanup failed for {job_id}: {redis_error}")
                
                cleanup_results.append({
                    "job_id": job_id,
                    "description": job_desc,
                    "status": "cleaned_up"
                })
        
        # Try to restart the job monitor (though it should already be running)
        try:
            import threading
            monitor_thread = threading.Thread(target=job_monitor)
            monitor_thread.daemon = True
            monitor_thread.start()
            monitor_status = "restarted"
        except Exception as monitor_error:
            print(f"⚠️ Failed to restart monitor: {monitor_error}")
            monitor_status = f"failed: {monitor_error}"
        
        # Check for pending/queued jobs that can now start
        try:
            start_next_queued_job()
            queue_check = "attempted_to_start_next_job"
        except Exception as queue_error:
            queue_check = f"queue_check_failed: {queue_error}"
        
        return {
            "message": f"Emergency cleanup completed. Found and cleaned up {len(orphaned_jobs.data)} orphaned jobs.",
            "orphaned_jobs_cleaned": len(orphaned_jobs.data),
            "cleanup_details": cleanup_results,
            "monitor_status": monitor_status,
            "queue_status": queue_check
        }
        
    except Exception as e:
        print(f"❌ Emergency cleanup error: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/jobs/force-kill-all")
async def force_kill_all_background_processes(current_user: str = Depends(verify_token)):
    """NUCLEAR OPTION: Force kill all background scraping processes"""
    try:
        import os
        import subprocess
        
        killed_processes = []
        cleanup_results = []
        
        # First, clean up database and Redis like emergency restart
        supabase = get_supabase_client()
        if supabase:
            # Mark all running jobs as failed
            running_jobs = supabase.table("scraper_jobs").select("id", "description").eq("status", JobStatus.RUNNING).execute()
            
            for job in running_jobs.data:
                job_id = job["id"]
                job_desc = job.get("description", f"Job {job_id}")
                
                # Mark as failed
                supabase.table("scraper_jobs").update({
                    "status": JobStatus.FAILED,
                    "error_message": "Force killed - background process termination",
                    "updated_at": datetime.utcnow().isoformat()
                }).eq("id", job_id).execute()
                
                # Clean up Redis data
                redis_client = get_redis_client()
                if redis_client:
                    try:
                        redis_client.delete(f"checkpoint:{job_id}")
                        redis_client.delete(f"rescrape_job:{job_id}")
                        redis_client.delete(f"csv_data:{job_id}")
                    except Exception:
                        pass
                
                cleanup_results.append({
                    "job_id": job_id,
                    "description": job_desc,
                    "status": "database_cleaned"
                })
        
        # Try to find and kill Python processes with scraping-related names
        try:
            # Kill processes by name patterns
            kill_patterns = [
                "UnifiedRescaper",
                "UnifiedScraper", 
                "rescrape_",
                "process_new_creators",
                "scraper_jobs"
            ]
            
            for pattern in kill_patterns:
                try:
                    # Use pkill to find and kill processes matching the pattern
                    result = subprocess.run(['pkill', '-f', pattern], 
                                          capture_output=True, text=True, timeout=10)
                    if result.returncode == 0:
                        killed_processes.append(f"Killed processes matching: {pattern}")
                    else:
                        killed_processes.append(f"No processes found for: {pattern}")
                except subprocess.TimeoutExpired:
                    killed_processes.append(f"Timeout killing: {pattern}")
                except Exception as kill_error:
                    killed_processes.append(f"Error killing {pattern}: {str(kill_error)}")
        
        except Exception as process_error:
            killed_processes.append(f"Process killing failed: {str(process_error)}")
        
        # Set a global flag to stop job monitor loops
        try:
            redis_client = get_redis_client()
            if redis_client:
                redis_client.set("FORCE_STOP_ALL_JOBS", "true", ex=300)  # 5 minute flag
        except Exception:
            pass
        
        return {
            "message": "Force kill executed - all background scraping processes terminated",
            "database_cleanup": len(cleanup_results),
            "cleanup_details": cleanup_results,
            "process_termination": killed_processes,
            "warning": "This is a nuclear option - restart your server to resume normal operation"
        }
        
    except Exception as e:
        print(f"❌ Force kill error: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/jobs/clear-force-stop-flag")
async def clear_force_stop_flag(current_user: str = Depends(verify_token)):
    """Clear the force stop flag that might be preventing jobs from starting"""
    try:
        redis_client = get_redis_client()
        if redis_client:
            # Check if flag exists
            flag_exists = redis_client.get("FORCE_STOP_ALL_JOBS")
            if flag_exists:
                redis_client.delete("FORCE_STOP_ALL_JOBS")
                message = "Force stop flag cleared - jobs can now start normally"
                print(f"🟢 {message}")
            else:
                message = "No force stop flag found - jobs should be able to start normally"
                print(f"ℹ️ {message}")
                
            # Try to start any pending jobs
            start_next_queued_job()
            
            return {
                "message": message,
                "flag_was_set": bool(flag_exists),
                "action": "attempted_to_start_queued_jobs"
            }
        else:
            raise HTTPException(status_code=500, detail="Redis connection failed")
            
    except Exception as e:
        print(f"❌ Clear force stop flag error: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/jobs/debug-status")
async def debug_job_status(current_user: str = Depends(verify_token)):
    """Debug endpoint to check current job execution status"""
    try:
        supabase = get_supabase_client()
        if not supabase:
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        # Get all recent jobs (last 10)
        recent_jobs = supabase.table("scraper_jobs").select("*").order("created_at", desc=True).limit(10).execute()
        
        # Get running jobs
        running_jobs = supabase.table("scraper_jobs").select("*").eq("status", JobStatus.RUNNING).execute()
        
        # Get pending/queued jobs
        pending_jobs = supabase.table("scraper_jobs").select("*").in_("status", [JobStatus.PENDING, JobStatus.QUEUED]).execute()
        
        # Check Redis flags
        redis_info = {}
        redis_client = get_redis_client()
        if redis_client:
            try:
                redis_info["force_stop_flag"] = bool(redis_client.get("FORCE_STOP_ALL_JOBS"))
                redis_info["queue_paused"] = redis_client.get("queue_paused") == "true"
                
                # Check if there are any Redis job keys
                redis_keys = redis_client.keys("rescrape_job:*")
                redis_info["active_job_keys"] = len(redis_keys)
                redis_info["job_keys"] = [key.decode() for key in redis_keys[:5]]  # Show first 5
            except Exception as redis_error:
                redis_info["error"] = str(redis_error)
        
        # Try to import tasks module to check for import issues
        tasks_import_status = "unknown"
        try:
            from tasks import rescrape_all_creators
            tasks_import_status = "success"
        except Exception as import_error:
            tasks_import_status = f"failed: {str(import_error)}"
        
        return {
            "recent_jobs": [
                {
                    "id": job["id"][:8],  # Shortened ID
                    "job_type": job["job_type"],
                    "status": job["status"],
                    "created_at": job["created_at"],
                    "error_message": job.get("error_message", "")[:100] if job.get("error_message") else None
                }
                for job in recent_jobs.data
            ],
            "running_jobs_count": len(running_jobs.data),
            "pending_queued_count": len(pending_jobs.data),
            "redis_info": redis_info,
            "tasks_import_status": tasks_import_status,
            "next_action_suggestion": (
                "Clear force stop flag" if redis_info.get("force_stop_flag") 
                else "Check task import error" if "failed" in tasks_import_status
                else "Try manual job start" if len(pending_jobs.data) > 0
                else "All looks normal - check server logs for job execution errors"
            )
        }
        
    except Exception as e:
        print(f"❌ Debug status error: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/rescraping/simple-stats")
async def get_simple_rescrape_stats(current_user: str = Depends(verify_token)):
    """Get simplified rescrape statistics for manual rescraping system"""
    try:
        supabase = get_supabase_client()
        if not supabase:
            raise HTTPException(status_code=500, detail="Database not available")
        
        # Get total creator count
        total_response = supabase.table("creatordata").select("id", count="exact").execute()
        total_creators = total_response.count or 0
        
        # Get the most recent rescrape date
        recent_response = supabase.table("creatordata").select("updated_at").order("updated_at", desc=True).limit(1).execute()
        
        last_rescrape_date = None
        days_since_rescrape = 0
        if recent_response.data and recent_response.data[0].get('updated_at'):
            last_rescrape_date = recent_response.data[0]['updated_at']
            try:
                last_date = datetime.fromisoformat(last_rescrape_date.replace('Z', '+00:00'))
                days_since_rescrape = (datetime.now(last_date.tzinfo) - last_date).days
            except:
                days_since_rescrape = 0
        
        # Calculate next recommended date (7 days from last rescrape)
        if last_rescrape_date:
            try:
                last_date = datetime.fromisoformat(last_rescrape_date.replace('Z', '+00:00'))
                next_recommended = last_date + timedelta(days=7)
            except:
                next_recommended = datetime.now() + timedelta(days=7)
        else:
            next_recommended = datetime.now()  # If never scraped, recommend now
        
        # Determine if overdue (more than 7 days since last rescrape)
        is_overdue = days_since_rescrape > 7
        overdue_days = max(0, days_since_rescrape - 7)
        
        return {
            "total_creators": total_creators,
            "last_rescrape_date": last_rescrape_date,
            "days_since_rescrape": days_since_rescrape,
            "next_recommended_date": next_recommended.isoformat(),
            "is_overdue": is_overdue,
            "overdue_days": overdue_days
        }
        
    except Exception as e:
        print(f"❌ Simple rescrape stats error: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/system/simple-status")
async def get_simple_system_status(current_user: str = Depends(verify_token)):
    """Get simple system status without complex monitoring"""
    try:
        # Basic system info
        supabase_ok = get_supabase_client() is not None
        redis_ok = get_redis_client() is not None
        scraper_ok = get_scraper() is not None
        
        # Count creators in database
        creator_count = 0
        if supabase_ok:
            try:
                supabase = get_supabase_client()
                response = supabase.table("creatordata").select("id", count="exact").execute()
                creator_count = response.count
            except:
                pass
        
        return {
            "timestamp": datetime.utcnow().isoformat(),
            "connections": {
                "supabase": supabase_ok,
                "redis": redis_ok,
                "scraper": scraper_ok
            },
            "data": {
                "total_creators": creator_count
            },
            "status": "healthy" if all([supabase_ok, redis_ok, scraper_ok]) else "degraded"
        }
        
    except Exception as e:
        print(f"❌ System status error: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

# ==================== SIMPLE DIRECT EXECUTION ENDPOINTS ====================

@app.post("/simple/rescrape-all")
async def simple_rescrape_all_creators(current_user: str = Depends(verify_token)):
    """Simple endpoint to rescrape all creators directly (no job queue)"""
    try:
        print("🚀 Starting simple rescrape all creators")
        
        # Get all creators from database
        supabase = get_supabase_client()
        if not supabase:
            raise HTTPException(status_code=500, detail="Database not available")
        
        response = supabase.table("creatordata").select("handle").execute()
        if not response.data:
            return {"message": "No creators found to rescrape", "successful": 0, "failed": 0}
        
        creator_handles = [creator["handle"] for creator in response.data]
        
        # Execute directly
        result = simple_rescrape_creators(creator_handles)
        return result
        
    except Exception as e:
        print(f"❌ Simple rescrape all error: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/simple/rescrape-platform/{platform}")
async def simple_rescrape_platform(platform: str, current_user: str = Depends(verify_token)):
    """Simple endpoint to rescrape creators from specific platform"""
    try:
        platform = platform.lower()
        if platform not in ['instagram', 'tiktok']:
            raise HTTPException(status_code=400, detail="Platform must be 'instagram' or 'tiktok'")
        
        print(f"🚀 Starting simple rescrape for {platform}")
        
        # Get platform creators from database
        supabase = get_supabase_client()
        if not supabase:
            raise HTTPException(status_code=500, detail="Database not available")
        
        response = supabase.table("creatordata").select("handle").eq("platform", platform.title()).execute()
        if not response.data:
            return {"message": f"No {platform} creators found to rescrape", "successful": 0, "failed": 0}
        
        creator_handles = [creator["handle"] for creator in response.data]
        
        # Execute directly
        result = simple_rescrape_creators(creator_handles)
        return result
        
    except Exception as e:
        print(f"❌ Simple rescrape platform error: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/simple/rescrape-overdue")
async def simple_rescrape_overdue_creators(days_old: int = 7, current_user: str = Depends(verify_token)):
    """Simple endpoint to rescrape creators older than specified days"""
    try:
        print(f"🚀 Starting simple rescrape for creators older than {days_old} days")
        
        # Get overdue creators from database
        supabase = get_supabase_client()
        if not supabase:
            raise HTTPException(status_code=500, detail="Database not available")
        
        cutoff_date = (datetime.utcnow() - timedelta(days=days_old)).isoformat()
        response = supabase.table("creatordata").select("handle").lt("updated_at", cutoff_date).execute()
        
        if not response.data:
            return {"message": f"No creators older than {days_old} days found", "successful": 0, "failed": 0}
        
        creator_handles = [creator["handle"] for creator in response.data]
        
        # Execute directly
        result = simple_rescrape_creators(creator_handles)
        result["message"] = f"Rescraped {len(creator_handles)} creators older than {days_old} days"
        return result
        
    except Exception as e:
        print(f"❌ Simple rescrape overdue error: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/simple/process-csv")
async def simple_process_csv_creators(file: UploadFile = File(...), current_user: str = Depends(verify_token)):
    """Simple endpoint to process new creators from CSV directly (no job queue)"""
    try:
        print("🚀 Starting simple CSV processing")
        
        # Validate file
        if not file.filename.endswith('.csv'):
            raise HTTPException(status_code=400, detail="File must be a CSV")
        
        # Read CSV
        contents = await file.read()
        df = pd.read_csv(io.StringIO(contents.decode('utf-8')))
        
        # Validate required columns
        required_columns = ['handle', 'platform']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise HTTPException(status_code=400, detail=f"Missing required columns: {missing_columns}")
        
        # Convert to list of dicts
        csv_data = df.to_dict('records')
        
        # Execute directly
        result = simple_process_new_creators(csv_data)
        return result
        
    except Exception as e:
        print(f"❌ Simple CSV processing error: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/simple/test-single-creator")
async def simple_test_single_creator(handle: str, platform: str, current_user: str = Depends(verify_token)):
    """Simple endpoint to test scraping a single creator"""
    try:
        handle = handle.strip().lstrip('@')
        platform = platform.lower()
        
        if platform not in ['instagram', 'tiktok']:
            raise HTTPException(status_code=400, detail="Platform must be 'instagram' or 'tiktok'")
        
        print(f"🧪 Testing single creator: @{handle} ({platform})")
        
        scraper = get_scraper()
        
        # Scrape creator data
        if platform == 'instagram':
            creator_data = scraper.scrape_instagram_creator(handle)
        else:
            creator_data = scraper.scrape_tiktok_creator(handle)
        
        if not creator_data:
            return {"status": "failed", "error": "Failed to scrape creator data"}
        
        if creator_data.get('error') == 'temporary':
            return {"status": "temporary_error", "message": creator_data.get('message', 'Temporary API error')}
        
        # Return basic info (don't save to database)
        basic_info = {
            'handle': creator_data.get('handle'),
            'platform': creator_data.get('platform'),
            'followers_count': creator_data.get('followers_count', 0),
            'average_views': creator_data.get('average_views', 0),
            'engagement_rate': creator_data.get('engagement_rate', 0),
            'is_active': creator_data.get('is_active', False),
            'activity_status': creator_data.get('activity_status', 'unknown')
        }
        
        return {"status": "success", "data": basic_info}
        
    except Exception as e:
        print(f"❌ Simple test creator error: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
