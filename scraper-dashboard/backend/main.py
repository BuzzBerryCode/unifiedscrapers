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
        
        # Get creators due for rescraping (updated > 7 days ago)
        seven_days_ago = (datetime.utcnow() - timedelta(days=7)).isoformat()
        due_response = supabase.table("creatordata").select("id", count="exact").lt("updated_at", seven_days_ago).execute()
        creators_due = due_response.count
        
        # Get creators by day of week for next 7 days
        schedule = {}
        for i in range(7):
            date = datetime.utcnow() + timedelta(days=i)
            day_name = date.strftime("%A")
            
            # Calculate how many creators would be due on this day
            # This is a rough estimate based on spreading creators evenly
            creators_for_day = (creators_due + creators_need_dates) // 7
            if i < (creators_due + creators_need_dates) % 7:
                creators_for_day += 1
                
            schedule[day_name] = {
                "date": date.strftime("%Y-%m-%d"),
                "day": day_name,
                "estimated_creators": creators_for_day
            }
        
        # Get recent rescraping jobs
        recent_jobs = supabase.table("scraper_jobs").select("*").in_("job_type", ["rescrape_all", "rescrape_instagram", "rescrape_tiktok"]).order("created_at", desc=True).limit(10).execute()
        
        return {
            "total_creators": total_creators,
            "creators_need_dates": creators_need_dates,
            "creators_due_rescrape": creators_due,
            "weekly_schedule": schedule,
            "recent_jobs": recent_jobs.data
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
        base_date = datetime.utcnow() - timedelta(days=7)  # Start from 7 days ago
        
        for i, creator in enumerate(null_creators.data):
            # Spread creators across 7 days
            days_offset = i % 7
            hours_offset = random.randint(0, 23)
            minutes_offset = random.randint(0, 59)
            
            updated_date = base_date + timedelta(
                days=days_offset,
                hours=hours_offset,
                minutes=minutes_offset
            )
            
            # Update the creator's updated_at date
            supabase.table("creatordata").update({
                "updated_at": updated_date.isoformat()
            }).eq("id", creator["id"]).execute()
            
            updated_count += 1
            
            # Progress update every 100 creators
            if updated_count % 100 == 0:
                print(f"📅 Updated {updated_count}/{len(null_creators.data)} creators")
        
        print(f"✅ Completed populating dates for {updated_count} creators")
        return {"message": f"Successfully populated dates for {updated_count} creators", "updated_count": updated_count}
        
    except Exception as e:
        print(f"Populate dates error: {e}")
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
        
        # Start background job monitor
        try:
            monitor_thread = threading.Thread(target=job_monitor)
            monitor_thread.daemon = True
            monitor_thread.start()
            print("✅ Background job monitor started")
        except Exception as monitor_error:
            print(f"⚠️ Background job monitor failed to start: {monitor_error}")
            # Don't fail startup if monitor fails
        
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

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
