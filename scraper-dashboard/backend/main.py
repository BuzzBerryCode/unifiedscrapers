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
            return False
        
        response = client.table("scraper_jobs").select("id").eq("status", "running").execute()
        return len(response.data) > 0
    except Exception as e:
        print(f"Error checking running jobs: {e}")
        return False

def start_next_queued_job():
    """Start the next queued job if no jobs are running."""
    try:
        if check_running_jobs():
            return  # Already have a running job
        
        client = get_supabase_client()
        if not client:
            return
        
        # Get the oldest queued job
        response = client.table("scraper_jobs").select("*").eq("status", "queued").order("created_at").limit(1).execute()
        
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
                    # Extract platform from description or default to Instagram
                    platform = "Instagram"
                    if "TikTok" in job.get("description", ""):
                        platform = "TikTok"
                    celery.send_task("tasks.rescrape_platform_creators", args=[job_id, platform])
                
                print(f"Started queued job: {job_id}")
    
    except Exception as e:
        print(f"Error starting next queued job: {e}")

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

@app.post("/jobs/start-queue")
async def start_queue(
    current_user: str = Depends(verify_token)
):
    """Manually trigger the job queue to start pending jobs."""
    try:
        start_next_queued_job()
        return {"message": "Job queue processing triggered"}
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
            else:
                raise HTTPException(status_code=400, detail="Resume not supported for this job type yet")
        
        return {
            "message": f"Job {job_id} resumed from index {resume_from_index}",
            "resume_from_index": resume_from_index,
            "total_items": job.get("total_items", 0)
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error resuming job: {str(e)}")

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
    print("✅ Scraper Dashboard API started")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
