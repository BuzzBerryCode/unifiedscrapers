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
from celery import Celery
from supabase import create_client, Client
import jwt
from passlib.context import CryptContext
import sys
import traceback

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
        except Exception as e:
            print(f"Supabase connection failed: {e}")
            supabase = None
    return supabase

# Redis & Celery - Initialize lazily to avoid startup failures
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
redis_client = None
celery_app = None

def get_redis_client():
    global redis_client
    if redis_client is None:
        try:
            redis_client = redis.from_url(REDIS_URL)
            # Test the connection
            redis_client.ping()
        except Exception as e:
            print(f"Redis connection failed: {e}")
            redis_client = None
    return redis_client

def get_celery_app():
    global celery_app
    if celery_app is None:
        try:
            celery_app = Celery(
                "scraper_tasks",
                broker=REDIS_URL,
                backend=REDIS_URL,
                include=["tasks"]
            )
        except Exception as e:
            print(f"Celery initialization failed: {e}")
            celery_app = None
    return celery_app

# ==================== MODELS ====================

from pydantic import BaseModel
from enum import Enum

class JobStatus(str, Enum):
    PENDING = "pending"
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    PAUSED = "paused"

class JobType(str, Enum):
    NEW_CREATORS = "new_creators"
    RESCRAPE_ALL = "rescrape_all"
    RESCRAPE_PLATFORM = "rescrape_platform"

class LoginRequest(BaseModel):
    username: str
    password: str

class JobRequest(BaseModel):
    job_type: JobType
    platform: Optional[str] = None
    description: Optional[str] = None

class JobResponse(BaseModel):
    id: str
    job_type: str
    status: str
    created_at: str
    updated_at: str
    description: str
    total_items: Optional[int] = None
    processed_items: Optional[int] = None
    failed_items: Optional[int] = None
    results: Optional[dict] = None
    error_message: Optional[str] = None

# ==================== AUTHENTICATION ====================

def verify_password(plain_password: str, hashed_password: str) -> bool:
    return pwd_context.verify(plain_password, hashed_password)

def get_password_hash(password: str) -> str:
    return pwd_context.hash(password)

def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(hours=24)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    try:
        payload = jwt.decode(credentials.credentials, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise HTTPException(status_code=401, detail="Invalid token")
        return username
    except jwt.PyJWTError:
        raise HTTPException(status_code=401, detail="Invalid token")

# ==================== DATABASE FUNCTIONS ====================

def init_job_table():
    """Initialize the jobs table in Supabase if it doesn't exist."""
    try:
        # Create jobs table
        client = get_supabase_client()
        if client:
            client.table("scraper_jobs").select("id").limit(1).execute()
    except:
        # Table doesn't exist, we'll create it via SQL
        print("Jobs table needs to be created in Supabase")

def check_running_jobs() -> bool:
    """Check if there are any currently running jobs."""
    try:
        client = get_supabase_client()
        if not client:
            print("❌ No Supabase client for running job check")
            return False
        
        response = client.table("scraper_jobs").select("id,job_type").eq("status", "running").execute()
        running_count = len(response.data)
        print(f"🔍 Found {running_count} running jobs")
        if running_count > 0:
            for job in response.data:
                print(f"   - Running: {job['id']} ({job['job_type']})")
        return running_count > 0
    except Exception as e:
        print(f"❌ Error checking running jobs: {e}")
        return False

def start_job_directly(job_id: str, job_type: str):
    """Start a job directly without Celery as a fallback mechanism"""
    import threading
    import asyncio
    import json
    import time
    
    print(f"🚀 Starting job {job_id} directly (bypassing Celery)")
    
    def run_job():
        try:
            # Import required modules
            sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
            from UnifiedScraper import process_instagram_user, process_tiktok_account
            
            client = get_supabase_client()
            if not client:
                raise Exception("Database connection failed")
            
            if job_type == "new_creators":
                # Get CSV data from Redis
                redis = get_redis_client()
                if not redis:
                    raise Exception("Redis connection failed")
                
                csv_data_json = redis.get(f"job_data:{job_id}")
                if not csv_data_json:
                    raise Exception("CSV data not found in Redis")
                
                csv_data = json.loads(csv_data_json)
                
                # Run the job processing logic directly
                total_items = len(csv_data)
                processed_items = 0
                failed_items = 0
                results = {"added": [], "failed": [], "skipped": [], "filtered": []}
                niche_stats = {"primary_niches": {}, "secondary_niches": {}}
                
                print(f"📋 Processing {total_items} creators directly")
                
                for i, creator_data in enumerate(csv_data):
                    try:
                        username = creator_data['Usernames'].strip()
                        platform = creator_data['Platform'].lower()
                        
                        print(f"   Processing {i + 1}/{total_items}: @{username} ({platform})")
                        
                        # Check if already exists
                        existing = client.table("creatordata").select("id", "platform", "primary_niche").eq("handle", username).execute()
                        if existing.data:
                            existing_creator = existing.data[0]
                            existing_platform = existing_creator.get('platform', 'Unknown')
                            existing_niche = existing_creator.get('primary_niche', 'Unknown')
                            results["skipped"].append(f"@{username} - Already exists in database ({existing_platform}, {existing_niche} niche)")
                            processed_items += 1
                            continue
                        
                        # Process creator
                        if platform == 'instagram':
                            result = asyncio.run(
                                asyncio.wait_for(
                                    asyncio.to_thread(process_instagram_user, username),
                                    timeout=300
                                )
                            )
                        elif platform == 'tiktok':
                            result = asyncio.run(
                                asyncio.wait_for(
                                    asyncio.to_thread(process_tiktok_account, username, "wjhGgI14NjNMUuXA92YWXjojozF2"),
                                    timeout=300
                                )
                            )
                        else:
                            results["failed"].append(f"@{username} - Unknown platform")
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
                        
                        processed_items += 1
                        
                        # Update progress
                        client.table("scraper_jobs").update({
                            "processed_items": processed_items,
                            "failed_items": failed_items,
                            "updated_at": datetime.utcnow().isoformat()
                        }).eq("id", job_id).execute()
                        
                        # Rate limiting
                        time.sleep(0.5)
                        
                    except Exception as e:
                        print(f"   ❌ Error processing @{username}: {e}")
                        results["failed"].append(f"@{username} - Error: {str(e)}")
                        failed_items += 1
                        processed_items += 1
                
                # Final update
                results["niche_stats"] = niche_stats
                client.table("scraper_jobs").update({
                    "status": "completed",
                    "processed_items": processed_items,
                    "failed_items": failed_items,
                    "results": results,
                    "updated_at": datetime.utcnow().isoformat()
                }).eq("id", job_id).execute()
                
                print(f"✅ Job {job_id} completed directly!")
                
                # Start next job
                start_next_queued_job()
                
            else:
                print(f"❌ Direct execution not implemented for job type: {job_type}")
                
        except Exception as e:
            print(f"❌ Direct job execution failed: {e}")
            # Update job status to failed
            try:
                client = get_supabase_client()
                if client:
                    client.table("scraper_jobs").update({
                        "status": "failed",
                        "error_message": str(e),
                        "updated_at": datetime.utcnow().isoformat()
                    }).eq("id", job_id).execute()
            except:
                pass
    
    # Start in background thread
    thread = threading.Thread(target=run_job, daemon=True)
    thread.start()
    print(f"🎯 Direct execution started for job {job_id}")

def start_next_queued_job():
    """Start the next queued job if no jobs are running and queue is not paused."""
    try:
        print("🔄 Checking for queued jobs...")
        
        # Check if queue is paused (with safe Redis access)
        try:
            redis = get_redis_client()
            if redis and redis.get("queue_paused") == b"true":
                print("⏸️ Queue is paused, skipping job start")
                return
        except Exception as e:
            print(f"⚠️ Redis check failed (continuing anyway): {e}")
        
        if check_running_jobs():
            print("⏸️ Already have a running job, skipping")
            return  # Already have a running job
        
        client = get_supabase_client()
        if not client:
            print("❌ No Supabase client available")
            return
        
        # Get the oldest pending or queued job (exclude completed, cancelled, failed, running, paused)
        response = client.table("scraper_jobs").select("*").in_("status", ["pending", "queued"]).order("created_at").limit(1).execute()
        print(f"📋 Found {len(response.data)} pending/queued jobs")
        
        if response.data:
            job = response.data[0]
            job_id = job["id"]
            job_type = job["job_type"]
            print(f"🚀 Starting job {job_id} ({job_type})")
            
            # Update status to running
            client.table("scraper_jobs").update({"status": "running", "updated_at": datetime.utcnow().isoformat()}).eq("id", job_id).execute()
            print(f"✅ Updated job {job_id} status to running")
            
            # Start the appropriate task with fallback to direct execution
            celery = get_celery_app()
            task_sent = False
            
            if celery:
                try:
                    if job["job_type"] == "new_creators":
                        print(f"📤 Sending new_creators task for job {job_id}")
                        celery.send_task("tasks.process_new_creators", args=[job_id])
                        task_sent = True
                    elif job["job_type"] == "rescrape_platform":
                        # Extract platform from description or default to Instagram
                        platform = "Instagram"
                        if "TikTok" in job.get("description", ""):
                            platform = "TikTok"
                        print(f"📤 Sending rescrape_platform task for job {job_id} ({platform})")
                        celery.send_task("tasks.rescrape_platform_creators", args=[job_id, platform])
                        task_sent = True
                    
                    if task_sent:
                        print(f"🎯 Celery task sent successfully for job {job_id}")
                        
                        # Wait a moment to see if the task is picked up
                        import time
                        time.sleep(2)
                        
                        # Check if job is actually progressing
                        job_check = client.table("scraper_jobs").select("processed_items", "updated_at").eq("id", job_id).execute()
                        if job_check.data:
                            # If no progress after 30 seconds, fall back to direct execution
                            import threading
                            def check_and_fallback():
                                time.sleep(30)
                                job_recheck = client.table("scraper_jobs").select("processed_items", "updated_at").eq("id", job_id).execute()
                                if job_recheck.data:
                                    job_data = job_recheck.data[0]
                                    # If still no progress, start direct execution
                                    if job_data.get("processed_items", 0) == 0:
                                        print(f"⚠️ Celery task not progressing, starting direct execution for {job_id}")
                                        start_job_directly(job_id, job["job_type"])
                            
                            # Start the fallback check in background
                            threading.Thread(target=check_and_fallback, daemon=True).start()
                    
                except Exception as e:
                    print(f"❌ Celery task dispatch failed: {e}")
                    task_sent = False
            
            if not task_sent:
                print("❌ Celery not available, starting job directly")
                start_job_directly(job_id, job["job_type"])
        else:
            print("📭 No pending/queued jobs found")
    
    except Exception as e:
        print(f"❌ Error starting next queued job: {e}")
        import traceback
        traceback.print_exc()

def create_job(job_type: str, description: str = "", total_items: int = 0) -> str:
    """Create a new job in the database with proper queuing."""
    job_id = str(uuid.uuid4())
    
    # Check if there are running jobs to determine initial status
    has_running_jobs = check_running_jobs()
    initial_status = JobStatus.QUEUED if has_running_jobs else JobStatus.PENDING
    
    job_data = {
        "id": job_id,
        "job_type": job_type,
        "status": initial_status,
        "description": description,
        "total_items": total_items,
        "processed_items": 0,
        "failed_items": 0,
        "created_at": datetime.utcnow().isoformat(),
        "updated_at": datetime.utcnow().isoformat()
    }
    
    try:
        client = get_supabase_client()
        if client:
            client.table("scraper_jobs").insert(job_data).execute()
            
            # If no running jobs, start this one immediately
            if not has_running_jobs:
                start_next_queued_job()
                
        return job_id
    except Exception as e:
        print(f"Error creating job: {e}")
        raise HTTPException(status_code=500, detail="Failed to create job")

def update_job_status(job_id: str, status: str, **kwargs):
    """Update job status and other fields."""
    update_data = {
        "status": status,
        "updated_at": datetime.utcnow().isoformat(),
        **kwargs
    }
    
    try:
        client = get_supabase_client()
        if client:
            client.table("scraper_jobs").update(update_data).eq("id", job_id).execute()
    except Exception as e:
        print(f"Error updating job {job_id}: {e}")

def get_jobs(limit: int = 50) -> List[dict]:
    """Get recent jobs from the database."""
    try:
        client = get_supabase_client()
        if not client:
            return []
        response = client.table("scraper_jobs").select("*").order("created_at", desc=True).limit(limit).execute()
        return response.data
    except Exception as e:
        print(f"Error fetching jobs: {e}")
        return []

# ==================== API ENDPOINTS ====================

@app.get("/")
async def root():
    return {"message": "Scraper Dashboard API", "version": "1.0.0"}

@app.get("/health")
async def health_check():
    """Health check endpoint for Railway"""
    try:
        # Basic health check - just return that the app is running
        return {
            "status": "healthy", 
            "timestamp": datetime.utcnow().isoformat(),
            "service": "Scraper Dashboard API"
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }

@app.get("/debug")
async def debug_env():
    """Debug endpoint to check environment variables"""
    # Test Supabase client creation with error details
    client_error = None
    try:
        from supabase import create_client
        url = os.getenv("SUPABASE_URL")
        key = os.getenv("SUPABASE_KEY")
        test_client = create_client(url, key)
        client_works = True
    except Exception as e:
        client_works = False
        client_error = str(e)
    
    return {
        "supabase_url_set": bool(os.getenv("SUPABASE_URL")),
        "supabase_key_set": bool(os.getenv("SUPABASE_KEY")),
        "redis_url_set": bool(os.getenv("REDIS_URL")),
        "supabase_url_length": len(os.getenv("SUPABASE_URL", "")),
        "supabase_key_length": len(os.getenv("SUPABASE_KEY", "")),
        "supabase_url": os.getenv("SUPABASE_URL", "")[:50] + "..." if os.getenv("SUPABASE_URL") else "",
        "client_creation_test": client_works,
        "client_error": client_error,
        "celery_connected": bool(get_celery_app()),
        "redis_ping": test_redis_connection()
    }

def test_redis_connection():
    """Test Redis connection"""
    try:
        redis_client = get_redis_client()
        if redis_client:
            redis_client.ping()
            return True
        return False
    except Exception as e:
        return f"Redis error: {str(e)}"

@app.post("/auth/login")
async def login(request: LoginRequest):
    """Admin login endpoint."""
    # Simple hardcoded admin for now - in production, use proper user management
    ADMIN_USERNAME = os.getenv("ADMIN_USERNAME", "admin")
    ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "scraper123")
    
    if request.username != ADMIN_USERNAME or request.password != ADMIN_PASSWORD:
        raise HTTPException(status_code=401, detail="Invalid credentials")
    
    access_token = create_access_token(data={"sub": request.username})
    return {"access_token": access_token, "token_type": "bearer"}

@app.post("/jobs/upload-csv")
async def upload_csv(
    file: UploadFile = File(...),
    current_user: str = Depends(verify_token)
):
    """Upload CSV file and create new creators job."""
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Only CSV files are allowed")
    
    try:
        # Read and parse CSV
        contents = await file.read()
        df = pd.read_csv(io.StringIO(contents.decode('utf-8')))
        
        # Validate CSV structure
        required_columns = ['Usernames', 'Platform']
        if not all(col in df.columns for col in required_columns):
            raise HTTPException(
                status_code=400, 
                detail=f"CSV must contain columns: {required_columns}"
            )
        
        # Clean and validate data
        df = df.dropna(subset=required_columns)
        df['Platform'] = df['Platform'].str.lower()
        valid_platforms = ['instagram', 'tiktok']
        df = df[df['Platform'].isin(valid_platforms)]
        
        if len(df) == 0:
            raise HTTPException(status_code=400, detail="No valid creators found in CSV")
        
        # Create job
        job_id = create_job(
            job_type=JobType.NEW_CREATORS,
            description=f"Process {len(df)} creators from {file.filename}",
            total_items=len(df)
        )
        
        # Store CSV data in Redis for processing
        csv_data = df.to_dict('records')
        redis = get_redis_client()
        if redis:
            redis.setex(f"job_data:{job_id}", 3600, json.dumps(csv_data))
        
        # The job will be started automatically by create_job if no jobs are running
        # If jobs are running, it will be queued and started when the current job completes
        
        return {
            "job_id": job_id,
            "message": f"Successfully queued job for {len(df)} creators",
            "creators_count": len(df)
        }
        
    except pd.errors.EmptyDataError:
        raise HTTPException(status_code=400, detail="CSV file is empty")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing CSV: {str(e)}")

@app.post("/jobs/rescrape")
async def create_rescrape_job(
    request: JobRequest,
    current_user: str = Depends(verify_token)
):
    """Create a rescraping job."""
    try:
        # Get creator count for the job
        if request.job_type == JobType.RESCRAPE_ALL:
            # Count all creators
            client = get_supabase_client()
            if not client:
                return {"job_id": job_id, "message": "Database connection failed", "creators_count": 0}
            response = client.table("creatordata").select("id", count="exact").execute()
            total_items = response.count
            description = f"Rescrape all {total_items} creators"
            
        elif request.job_type == JobType.RESCRAPE_PLATFORM:
            if not request.platform:
                raise HTTPException(status_code=400, detail="Platform is required for platform rescraping")
            
            # Count creators for specific platform
            client = get_supabase_client()
            if not client:
                return {"job_id": job_id, "message": "Database connection failed", "creators_count": 0}
            response = client.table("creatordata").select("id", count="exact").eq("platform", request.platform.title()).execute()
            total_items = response.count
            description = f"Rescrape all {total_items} {request.platform.title()} creators"
        
        else:
            raise HTTPException(status_code=400, detail="Invalid job type")
        
        # Create job
        job_id = create_job(
            job_type=request.job_type,
            description=description,
            total_items=total_items
        )
        
        # Queue the job
        celery = get_celery_app()
        if celery:
            if request.job_type == JobType.RESCRAPE_ALL:
                celery.send_task("tasks.rescrape_all_creators", args=[job_id])
            else:
                celery.send_task("tasks.rescrape_platform_creators", args=[job_id, request.platform])
        
        return {
            "job_id": job_id,
            "message": f"Successfully queued {request.job_type} job",
            "total_items": total_items
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error creating job: {str(e)}")

@app.get("/jobs", response_model=List[JobResponse])
async def get_job_list(
    limit: int = 50,
    current_user: str = Depends(verify_token)
):
    """Get list of jobs."""
    jobs = get_jobs(limit)
    return jobs

@app.get("/jobs/{job_id}", response_model=JobResponse)
async def get_job_details(
    job_id: str,
    current_user: str = Depends(verify_token)
):
    """Get details of a specific job."""
    try:
        client = get_supabase_client()
        if not client:
            raise HTTPException(status_code=500, detail="Database connection failed")
        response = client.table("scraper_jobs").select("*").eq("id", job_id).execute()
        if not response.data:
            raise HTTPException(status_code=404, detail="Job not found")
        return response.data[0]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching job: {str(e)}")

@app.delete("/jobs/{job_id}")
async def cancel_job(
    job_id: str,
    current_user: str = Depends(verify_token)
):
    """Cancel a job."""
    try:
        # Update job status to cancelled
        update_job_status(job_id, JobStatus.CANCELLED)
        
        # Try to revoke the Celery task
        celery = get_celery_app()
        if celery:
            celery.control.revoke(job_id, terminate=True)
        
        return {"message": "Job cancelled successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error cancelling job: {str(e)}")

@app.delete("/jobs/{job_id}/remove")
async def remove_job(
    job_id: str,
    current_user: str = Depends(verify_token)
):
    """Permanently remove a job from the database."""
    try:
        client = get_supabase_client()
        if not client:
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        # Check if job exists and is not running
        job_response = client.table("scraper_jobs").select("*").eq("id", job_id).execute()
        if not job_response.data:
            raise HTTPException(status_code=404, detail="Job not found")
        
        job = job_response.data[0]
        if job["status"] == "running":
            raise HTTPException(status_code=400, detail="Cannot remove a running job. Cancel it first.")
        
        # Delete the job from database
        client.table("scraper_jobs").delete().eq("id", job_id).execute()
        
        return {"message": "Job removed successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error removing job: {str(e)}")

@app.post("/jobs/pause-queue")
async def pause_queue(
    current_user: str = Depends(verify_token)
):
    """Pause the job queue by setting all queued jobs to paused status."""
    try:
        client = get_supabase_client()
        if not client:
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        # Update all queued jobs to paused status
        response = client.table("scraper_jobs").update({"status": "paused"}).eq("status", "queued").execute()
        paused_count = len(response.data) if response.data else 0
        
        # Store queue pause state in Redis
        try:
            redis = get_redis_client()
            if redis:
                redis.set("queue_paused", "true")
        except Exception as e:
            print(f"⚠️ Redis pause state storage failed: {e}")
        
        return {"message": f"Queue paused. {paused_count} jobs moved to paused status."}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error pausing queue: {str(e)}")

@app.post("/jobs/resume-queue") 
async def resume_queue(
    current_user: str = Depends(verify_token)
):
    """Resume the job queue by setting all paused jobs back to queued status."""
    try:
        client = get_supabase_client()
        if not client:
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        # Update all paused jobs back to queued status
        response = client.table("scraper_jobs").update({"status": "queued"}).eq("status", "paused").execute()
        resumed_count = len(response.data) if response.data else 0
        
        # Remove queue pause state from Redis
        try:
            redis = get_redis_client()
            if redis:
                redis.delete("queue_paused")
        except Exception as e:
            print(f"⚠️ Redis pause state removal failed: {e}")
        
        # Try to start the next job if no jobs are currently running
        if not check_running_jobs():
            start_next_queued_job()
        
        return {"message": f"Queue resumed. {resumed_count} jobs moved back to queued status."}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error resuming queue: {str(e)}")

@app.get("/jobs/running")
async def get_running_jobs(
    current_user: str = Depends(verify_token)
):
    """Get detailed information about all running jobs."""
    try:
        client = get_supabase_client()
        if not client:
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        # Get all running jobs with full details
        response = client.table("scraper_jobs").select("*").eq("status", "running").execute()
        
        return {
            "running_jobs": response.data,
            "count": len(response.data)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting running jobs: {str(e)}")

@app.get("/jobs/queue-status")
async def get_queue_status(
    current_user: str = Depends(verify_token)
):
    """Get the current status of the job queue."""
    try:
        client = get_supabase_client()
        if not client:
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        # Check if queue is paused
        is_paused = False
        try:
            redis = get_redis_client()
            if redis:
                is_paused = redis.get("queue_paused") == b"true"
        except Exception as e:
            print(f"⚠️ Redis pause check failed: {e}")
        
        # Get counts of jobs by status
        running_jobs = client.table("scraper_jobs").select("id", count="exact").eq("status", "running").execute()
        queued_jobs = client.table("scraper_jobs").select("id", count="exact").eq("status", "queued").execute()
        paused_jobs = client.table("scraper_jobs").select("id", count="exact").eq("status", "paused").execute()
        pending_jobs = client.table("scraper_jobs").select("id", count="exact").eq("status", "pending").execute()
        
        return {
            "queue_paused": is_paused,
            "running_jobs": running_jobs.count,
            "queued_jobs": queued_jobs.count,
            "paused_jobs": paused_jobs.count,
            "pending_jobs": pending_jobs.count,
            "total_active": running_jobs.count + queued_jobs.count + pending_jobs.count
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting queue status: {str(e)}")

@app.post("/jobs/start-queue")
async def start_queue(
    current_user: str = Depends(verify_token)
):
    """Manually trigger the job queue to start pending jobs."""
    try:
        # Check if queue is paused
        try:
            redis = get_redis_client()
            if redis and redis.get("queue_paused") == b"true":
                raise HTTPException(status_code=400, detail="Queue is paused. Resume queue first.")
        except Exception as e:
            print(f"⚠️ Redis pause check failed (continuing anyway): {e}")
        
        # Force start the next job regardless of running job check
        client = get_supabase_client()
        if not client:
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        # Get the oldest pending or queued job (exclude completed, cancelled, failed, running, paused)
        response = client.table("scraper_jobs").select("*").in_("status", ["pending", "queued"]).order("created_at").limit(1).execute()
        
        if response.data:
            job = response.data[0]
            job_id = job["id"]
            
            # Update status to running
            client.table("scraper_jobs").update({"status": "running", "updated_at": datetime.utcnow().isoformat()}).eq("id", job_id).execute()
            
            # Start the appropriate Celery task
            celery = get_celery_app()
            if celery:
                if job["job_type"] == "new_creators":
                    celery.send_task("tasks.process_new_creators", args=[job_id])
                elif job["job_type"] == "rescrape_platform":
                    platform = "Instagram"
                    if "TikTok" in job.get("description", ""):
                        platform = "TikTok"
                    celery.send_task("tasks.rescrape_platform_creators", args=[job_id, platform])
                
                return {"message": f"Started job {job_id} ({job['job_type']})"}
            else:
                raise HTTPException(status_code=500, detail="Celery not available")
        else:
            return {"message": "No pending jobs found"}
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error starting queue: {str(e)}")

@app.post("/jobs/{job_id}/resume")
async def resume_job(
    job_id: str,
    current_user: str = Depends(verify_token)
):
    """Resume a cancelled or failed job from where it left off."""
    try:
        client = get_supabase_client()
        if not client:
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        # Get the job details
        job_response = client.table("scraper_jobs").select("*").eq("id", job_id).execute()
        if not job_response.data:
            raise HTTPException(status_code=404, detail="Job not found")
        
        job = job_response.data[0]
        
        # Check if job can be resumed
        if job["status"] not in ["cancelled", "failed"]:
            raise HTTPException(status_code=400, detail=f"Job cannot be resumed (current status: {job['status']})")
        
        # Get resume point from processed_items
        resume_from_index = job.get("processed_items", 0)
        
        # Update job status to running
        client.table("scraper_jobs").update({"status": "running"}).eq("id", job_id).execute()
        
        # Queue the job with resume index
        celery = get_celery_app()
        if celery:
            if job["job_type"] == "rescrape_platform":
                # Extract platform from description or job data
                platform = "Instagram"  # Default, should be extracted from job data
                if "TikTok" in job.get("description", ""):
                    platform = "TikTok"
                
                celery.send_task("tasks.rescrape_platform_creators", args=[job_id, platform, resume_from_index])
            elif job["job_type"] == "new_creators":
                # Resume new creators job from where it left off
                celery.send_task("tasks.process_new_creators", args=[job_id, resume_from_index])
                print(f"🔄 Resumed new_creators job {job_id} from index {resume_from_index}")
            else:
                raise HTTPException(status_code=400, detail=f"Resume not supported for job type: {job['job_type']}")
        
        return {
            "message": f"Job {job_id} resumed from index {resume_from_index}",
            "resume_from_index": resume_from_index,
            "total_items": job.get("total_items", 0)
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error resuming job: {str(e)}")

@app.post("/jobs/{job_id}/force-continue")
async def force_continue_job(
    job_id: str,
    current_user: str = Depends(verify_token)
):
    """Force continue a stuck running job by restarting it."""
    try:
        client = get_supabase_client()
        if not client:
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        # Get the job details
        job_response = client.table("scraper_jobs").select("*").eq("id", job_id).execute()
        if not job_response.data:
            raise HTTPException(status_code=404, detail="Job not found")
        
        job = job_response.data[0]
        
        # Allow for running, failed, or cancelled jobs
        if job["status"] not in ["running", "failed", "cancelled"]:
            raise HTTPException(status_code=400, detail=f"Job cannot be force continued (current status: {job['status']})")
        
        # Get current progress to resume from
        resume_from_index = job.get("processed_items", 0)
        
        # Update job status to running
        client.table("scraper_jobs").update({
            "status": "running",
            "updated_at": datetime.utcnow().isoformat()
        }).eq("id", job_id).execute()
        
        # Force restart the job from current position
        celery = get_celery_app()
        if celery:
            if job["job_type"] == "new_creators":
                celery.send_task("tasks.process_new_creators", args=[job_id, resume_from_index])
                print(f"🔄 Force continued new_creators job {job_id} from index {resume_from_index}")
            elif job["job_type"] == "rescrape_platform":
                platform = "Instagram"
                if "TikTok" in job.get("description", ""):
                    platform = "TikTok"
                celery.send_task("tasks.rescrape_platform_creators", args=[job_id, platform, resume_from_index])
                print(f"🔄 Force continued rescrape job {job_id} from index {resume_from_index}")
            else:
                raise HTTPException(status_code=400, detail=f"Force continue not supported for job type: {job['job_type']}")
        else:
            raise HTTPException(status_code=500, detail="Celery not available")
        
        return {
            "message": f"Job {job_id} force continued from index {resume_from_index}",
            "resume_from_index": resume_from_index,
            "total_items": job.get("total_items", 0)
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error force continuing job: {str(e)}")

@app.post("/jobs/{job_id}/direct-restart")
async def direct_restart_job(
    job_id: str,
    current_user: str = Depends(verify_token)
):
    """Directly restart a job by running it in a background thread (bypasses Celery)."""
    import threading
    import asyncio
    from tasks import process_new_creators
    
    try:
        client = get_supabase_client()
        if not client:
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        # Get the job details
        job_response = client.table("scraper_jobs").select("*").eq("id", job_id).execute()
        if not job_response.data:
            raise HTTPException(status_code=404, detail="Job not found")
        
        job = job_response.data[0]
        resume_from_index = job.get("processed_items", 0)
        
        # Update job status to running
        client.table("scraper_jobs").update({
            "status": "running",
            "updated_at": datetime.utcnow().isoformat()
        }).eq("id", job_id).execute()
        
        # Run the job directly in a background thread
        def run_job_directly():
            try:
                if job["job_type"] == "new_creators":
                    # Create a new event loop for this thread
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    
                    # Import and run the task function directly
                    from tasks import process_new_creators
                    task_instance = process_new_creators()
                    task_instance.apply(args=[job_id, resume_from_index])
                    
                    loop.close()
                else:
                    print(f"Direct restart not implemented for job type: {job['job_type']}")
            except Exception as e:
                print(f"❌ Direct job execution failed: {e}")
                # Update job status to failed
                try:
                    client.table("scraper_jobs").update({
                        "status": "failed",
                        "error_message": str(e),
                        "updated_at": datetime.utcnow().isoformat()
                    }).eq("id", job_id).execute()
                except:
                    pass
        
        # Start the job in a background thread
        thread = threading.Thread(target=run_job_directly, daemon=True)
        thread.start()
        
        return {
            "message": f"Job {job_id} started directly (bypassing Celery)",
            "resume_from_index": resume_from_index,
            "total_items": job.get("total_items", 0)
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error directly restarting job: {str(e)}")

@app.post("/jobs/force-start-pending")
async def force_start_pending_job(
    current_user: str = Depends(verify_token)
):
    """Force start the pending job from position 380 using direct execution."""
    import threading
    import asyncio
    import json
    import time
    from datetime import datetime
    
    try:
        client = get_supabase_client()
        if not client:
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        # Get the specific pending job
        job_id = "df295d3d-22ab-4dcd-91a5-f24838cee348"
        resume_from_index = 380
        
        # Update job status and position
        client.table("scraper_jobs").update({
            "status": "running",
            "processed_items": resume_from_index,
            "updated_at": datetime.utcnow().isoformat()
        }).eq("id", job_id).execute()
        
        # Run job directly in background thread (bypassing problematic Celery)
        def run_job_directly():
            try:
                print(f"🚀 Starting direct job execution for {job_id}")
                
                # Get CSV data from Redis
                redis_client = get_redis_client()
                if not redis_client:
                    raise Exception("Redis connection failed")
                
                csv_data_json = redis_client.get(f"job_data:{job_id}")
                if not csv_data_json:
                    raise Exception("CSV data not found in Redis")
                
                csv_data = json.loads(csv_data_json)
                
                # Resume from specific index
                if resume_from_index > 0:
                    csv_data = csv_data[resume_from_index:]
                
                # Import scraper functions
                sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
                from UnifiedScraper import process_instagram_user, process_tiktok_account
                
                # Initialize tracking
                total_items = len(csv_data) + resume_from_index
                processed_items = resume_from_index
                failed_items = 0
                results = {"added": [], "failed": [], "skipped": [], "filtered": []}
                niche_stats = {"primary_niches": {}, "secondary_niches": {}}
                
                # Process creators
                for i, creator_data in enumerate(csv_data):
                    username = creator_data['Usernames'].strip()
                    platform = creator_data['Platform'].lower()
                    current_index = resume_from_index + i
                    
                    print(f"Processing {current_index + 1}/{total_items}: @{username} ({platform})")
                    
                    # Check if already exists
                    existing = client.table("creatordata").select("id", "platform", "primary_niche").eq("handle", username).execute()
                    if existing.data:
                        existing_creator = existing.data[0]
                        existing_platform = existing_creator.get('platform', 'Unknown')
                        existing_niche = existing_creator.get('primary_niche', 'Unknown')
                        results["skipped"].append(f"@{username} - Already exists in database ({existing_platform}, {existing_niche} niche)")
                        processed_items += 1
                        continue
                    
                    # Process creator with timeout
                    try:
                        if platform == 'instagram':
                            result = asyncio.run(
                                asyncio.wait_for(
                                    asyncio.to_thread(process_instagram_user, username),
                                    timeout=300  # 5 minute timeout per creator
                                )
                            )
                        elif platform == 'tiktok':
                            result = asyncio.run(
                                asyncio.wait_for(
                                    asyncio.to_thread(process_tiktok_account, username, "wjhGgI14NjNMUuXA92YWXjojozF2"),
                                    timeout=300
                                )
                            )
                        else:
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
                        results["failed"].append(f"@{username} - Timeout (5 minutes)")
                        failed_items += 1
                    except Exception as e:
                        print(f"❌ Error processing @{username}: {e}")
                        results["failed"].append(f"@{username} - Error: {str(e)}")
                        failed_items += 1
                    
                    processed_items += 1
                    
                    # Update progress every item
                    client.table("scraper_jobs").update({
                        "processed_items": processed_items,
                        "failed_items": failed_items,
                        "updated_at": datetime.utcnow().isoformat()
                    }).eq("id", job_id).execute()
                    
                    # Store intermediate results every 5 items
                    if i % 5 == 0:
                        intermediate_results = {
                            "added": results["added"].copy(),
                            "failed": results["failed"].copy(),
                            "skipped": results["skipped"].copy(),
                            "filtered": results["filtered"].copy(),
                            "niche_stats": niche_stats.copy()
                        }
                        client.table("scraper_jobs").update({
                            "status": "running",
                            "processed_items": processed_items,
                            "failed_items": failed_items,
                            "results": intermediate_results,
                            "updated_at": datetime.utcnow().isoformat()
                        }).eq("id", job_id).execute()
                        print(f"📊 CHECKPOINT: {processed_items}/{total_items} creators processed")
                    
                    # Rate limiting - intelligent delays
                    if result and isinstance(result, dict) and result.get("error") == "api_error":
                        time.sleep(2)  # Longer delay after API errors
                    else:
                        time.sleep(0.5)  # Shorter delay for successful calls
                
                # Final update
                results["niche_stats"] = niche_stats
                client.table("scraper_jobs").update({
                    "status": "completed",
                    "processed_items": processed_items,
                    "failed_items": failed_items,
                    "results": results,
                    "updated_at": datetime.utcnow().isoformat()
                }).eq("id", job_id).execute()
                
                print(f"🎉 Job {job_id} completed successfully!")
                print(f"   Added: {len(results['added'])}")
                print(f"   Skipped: {len(results['skipped'])}")
                print(f"   Filtered: {len(results['filtered'])}")
                print(f"   Failed: {len(results['failed'])}")
                
            except Exception as e:
                print(f"❌ Direct job execution failed: {e}")
                traceback.print_exc()
                # Update job status to failed
                try:
                    client.table("scraper_jobs").update({
                        "status": "failed",
                        "error_message": str(e),
                        "updated_at": datetime.utcnow().isoformat()
                    }).eq("id", job_id).execute()
                except:
                    pass
        
        # Start the job in a background thread
        thread = threading.Thread(target=run_job_directly, daemon=True)
        thread.start()
        
        return {
            "message": f"Job {job_id} started directly from position {resume_from_index} (bypassing Celery)",
            "method": "direct_execution",
            "resume_from_index": resume_from_index,
            "remaining_items": 507 - resume_from_index,
            "note": "Job is running in background thread on server"
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error force starting job: {str(e)}")

@app.post("/jobs/{job_id}/trigger")
async def trigger_job(job_id: str, current_user: str = Depends(verify_token)):
    """Manually trigger a pending job for testing."""
    try:
        # Get the job details
        client = get_supabase_client()
        if not client:
            raise HTTPException(status_code=500, detail="Database connection failed")
            
        job_response = client.table("scraper_jobs").select("*").eq("id", job_id).execute()
        if not job_response.data:
            raise HTTPException(status_code=404, detail="Job not found")
            
        job = job_response.data[0]
        if job["status"] != "pending":
            raise HTTPException(status_code=400, detail=f"Job is not pending (current status: {job['status']})")
        
        # Try to trigger the job manually
        celery = get_celery_app()
        if not celery:
            raise HTTPException(status_code=500, detail="Celery not available")
            
        # Import and call the task directly
        from tasks import process_new_creators_task, process_rescrape_task
        
        if job["job_type"] == "new_creators":
            result = process_new_creators_task.delay(job_id, job["results"])
        elif job["job_type"] == "rescrape_all":
            result = process_rescrape_task.delay(job_id)
        else:
            raise HTTPException(status_code=400, detail=f"Unknown job type: {job['job_type']}")
            
        return {"message": f"Job {job_id} triggered manually", "task_id": result.id}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error triggering job: {str(e)}")

@app.get("/creators/check/{handle}")
async def check_creator_data(
    handle: str,
    current_user: str = Depends(verify_token)
):
    """Check a specific creator's data including location."""
    try:
        client = get_supabase_client()
        if not client:
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        response = client.table("creatordata").select("handle,location,primary_niche,secondary_niche,platform,created_at").eq("handle", handle).execute()
        
        if not response.data:
            raise HTTPException(status_code=404, detail="Creator not found")
        
        return response.data[0]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching creator: {str(e)}")

@app.get("/stats")
async def get_dashboard_stats(current_user: str = Depends(verify_token)):
    """Get dashboard statistics."""
    try:
        # Get total creators
        client = get_supabase_client()
        if not client:
            raise HTTPException(status_code=500, detail="Database connection failed")
            
        creators_response = client.table("creatordata").select("id", count="exact").execute()
        total_creators = creators_response.count
        
        # Get creators by platform
        instagram_response = client.table("creatordata").select("id", count="exact").eq("platform", "Instagram").execute()
        tiktok_response = client.table("creatordata").select("id", count="exact").eq("platform", "TikTok").execute()
        
        # Get recent jobs
        recent_jobs = get_jobs(10)
        
        # Get job status counts
        job_stats = {}
        for status in JobStatus:
            jobs_response = client.table("scraper_jobs").select("id", count="exact").eq("status", status).execute()
            job_stats[status] = jobs_response.count
        
        return {
            "total_creators": total_creators,
            "instagram_creators": instagram_response.count,
            "tiktok_creators": tiktok_response.count,
            "recent_jobs": recent_jobs,
            "job_stats": job_stats
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching stats: {str(e)}")

# ==================== STARTUP ====================

@app.on_event("startup")
async def startup_event():
    """Initialize the application."""
    import os
    port = os.getenv("PORT", "8000")
    print("🚀 Scraper Dashboard API starting up...")
    print(f"📊 Supabase URL: {SUPABASE_URL}")
    print(f"🔗 Redis URL: {REDIS_URL}")
    print(f"🌐 Port: {port}")
    init_job_table()
    
    # Start automatic job processing (bypassing Celery entirely)
    import threading
    import time
    
    def job_monitor():
        """Background job monitor that processes pending jobs"""
        print("🔄 Starting background job monitor...")
        while True:
            try:
                # Check for pending jobs every 10 seconds
                time.sleep(10)
                
                client = get_supabase_client()
                if client:
                    # Get pending jobs
                    response = client.table("scraper_jobs").select("*").eq("status", "pending").order("created_at").limit(1).execute()
                    
                    if response.data:
                        job = response.data[0]
                        job_id = job["id"]
                        job_type = job["job_type"]
                        
                        print(f"🎯 Found pending job: {job_id} ({job_type})")
                        
                        # Check if no jobs are running
                        running_response = client.table("scraper_jobs").select("id").eq("status", "running").execute()
                        if not running_response.data:
                            print(f"🚀 Starting pending job {job_id}")
                            
                            # Update to running
                            client.table("scraper_jobs").update({
                                "status": "running",
                                "updated_at": datetime.utcnow().isoformat()
                            }).eq("id", job_id).execute()
                            
                            # Start job directly (bypassing Celery entirely)
                            start_job_directly(job_id, job_type)
                        else:
                            print(f"⏸️ Job {job_id} waiting (another job is running)")
                
            except Exception as e:
                print(f"❌ Job monitor error: {e}")
                time.sleep(30)  # Wait longer on errors
    
    # Start the job monitor in background
    monitor_thread = threading.Thread(target=job_monitor, daemon=True)
    monitor_thread.start()
    print("✅ Background job monitor started")
    
    print("✅ Scraper Dashboard API started")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
