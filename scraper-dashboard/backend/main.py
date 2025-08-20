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
            start_next_queued_job()
            time.sleep(10)  # Check every 10 seconds
        except Exception as e:
            print(f"❌ Job monitor error: {e}")
            time.sleep(30)  # Wait longer on error

# ==================== API ENDPOINTS ====================

@app.get("/health")
async def health_check():
    return {"status": "healthy", "timestamp": datetime.utcnow().isoformat()}

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
        
        # Validate CSV structure
        required_columns = ['username', 'platform']
        if not all(col in df.columns for col in required_columns):
            raise HTTPException(status_code=400, detail=f"CSV must contain columns: {required_columns}")
        
        # Create job
        job_id = str(uuid.uuid4())
        
        supabase = get_supabase_client()
        if not supabase:
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        # Store CSV data in Redis
        redis_client = get_redis_client()
        if redis_client:
            csv_data = df.to_dict('records')
            redis_client.setex(f"csv_data:{job_id}", 3600, json.dumps(csv_data))  # 1 hour expiry
        
        # Determine if there are running jobs
        running_jobs = check_running_jobs()
        initial_status = JobStatus.QUEUED if running_jobs else JobStatus.PENDING
        
        # Create job record
        job_data = {
            "id": job_id,
            "job_type": "new_creators",
            "status": initial_status,
            "total_items": len(df),
            "processed_items": 0,
            "created_at": datetime.utcnow().isoformat(),
            "updated_at": datetime.utcnow().isoformat()
        }
        
        supabase.table("scraper_jobs").insert(job_data).execute()
        
        # Start next job if none running
        if not running_jobs:
            start_next_queued_job()
        
        return {"job_id": job_id, "status": initial_status, "total_creators": len(df)}
        
    except Exception as e:
        print(f"Upload error: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

# ==================== STARTUP EVENT ====================

@app.on_event("startup")
async def startup_event():
    print("🚀 Scraper Dashboard API starting up...")
    print(f"📊 Supabase URL: {SUPABASE_URL}")
    print(f"🔗 Redis URL: {REDIS_URL}")
    print(f"🌐 Port: {os.getenv('PORT', '8000')}")
    
    # Test connections
    get_supabase_client()
    get_redis_client()
    
    # Start background job monitor
    monitor_thread = threading.Thread(target=job_monitor)
    monitor_thread.daemon = True
    monitor_thread.start()
    print("✅ Background job monitor started")
    
    print("✅ Scraper Dashboard API started")

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
