#!/usr/bin/env python3
"""
Job Management Script
Helps identify and manage running jobs
"""

import requests
import json
import sys
from datetime import datetime

# Configuration - Update with your actual Railway URL
API_BASE_URL = "https://scraper-dashboard-backend-production.up.railway.app"
USERNAME = "admin"  # Update if different
PASSWORD = "buzzberry123"  # Update if different

def login():
    """Get authentication token"""
    response = requests.post(f"{API_BASE_URL}/login", json={
        "username": USERNAME,
        "password": PASSWORD
    })
    
    if response.status_code == 200:
        return response.json()["access_token"]
    else:
        print(f"Login failed: {response.status_code}")
        print(response.text)
        sys.exit(1)

def get_headers(token):
    """Get headers with authentication"""
    return {"Authorization": f"Bearer {token}"}

def get_running_jobs(token):
    """Get all running jobs"""
    response = requests.get(f"{API_BASE_URL}/jobs/running", headers=get_headers(token))
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to get running jobs: {response.status_code}")
        print(response.text)
        return None

def cancel_job(token, job_id):
    """Cancel a specific job"""
    response = requests.delete(f"{API_BASE_URL}/jobs/{job_id}", headers=get_headers(token))
    
    if response.status_code == 200:
        print(f"✅ Successfully cancelled job {job_id}")
        return True
    else:
        print(f"❌ Failed to cancel job {job_id}: {response.status_code}")
        print(response.text)
        return False

def force_continue_job(token, job_id):
    """Force continue a stuck job"""
    response = requests.post(f"{API_BASE_URL}/jobs/{job_id}/force-continue", headers=get_headers(token))
    
    if response.status_code == 200:
        result = response.json()
        print(f"🚀 Successfully force continued job {job_id}")
        print(f"   Resume from index: {result['resume_from_index']}")
        print(f"   Total items: {result['total_items']}")
        return True
    else:
        print(f"❌ Failed to force continue job {job_id}: {response.status_code}")
        print(response.text)
        return False

def main():
    print("🔧 Job Management Script")
    print("=" * 50)
    
    # Login
    print("🔑 Logging in...")
    token = login()
    print("✅ Login successful")
    
    # Get running jobs
    print("\n📋 Getting running jobs...")
    result = get_running_jobs(token)
    
    if not result:
        print("❌ Failed to get running jobs")
        sys.exit(1)
    
    jobs = result["running_jobs"]
    print(f"🔍 Found {len(jobs)} running jobs:")
    
    # Display jobs
    for i, job in enumerate(jobs, 1):
        job_type = job["job_type"]
        description = job["description"]
        processed = job.get("processed_items", 0)
        total = job.get("total_items", 0)
        progress = (processed / total * 100) if total > 0 else 0
        created = job["created_at"]
        
        print(f"\n{i}. Job ID: {job['id']}")
        print(f"   Type: {job_type}")
        print(f"   Description: {description}")
        print(f"   Progress: {processed}/{total} ({progress:.1f}%)")
        print(f"   Created: {created}")
        
        # Identify jobs by description patterns
        if "rescrape" in description.lower() and "instagram" in description.lower():
            print("   🎯 THIS IS THE RESCRAPER - Should be cancelled")
        elif "crypto" in description.lower() and "trading" in description.lower():
            print("   🎯 THIS IS THE NEW CREATORS JOB - Should be force continued")
    
    print("\n" + "=" * 50)
    print("RECOMMENDED ACTIONS:")
    
    for job in jobs:
        description = job["description"]
        job_id = job["id"]
        
        if "rescrape" in description.lower() and "instagram" in description.lower():
            print(f"\n❌ CANCEL RESCRAPER:")
            print(f"   Job ID: {job_id}")
            answer = input("   Cancel this job? (y/N): ").lower()
            if answer == 'y':
                cancel_job(token, job_id)
        
        elif "crypto" in description.lower() and "trading" in description.lower():
            print(f"\n🚀 FORCE CONTINUE NEW CREATORS:")
            print(f"   Job ID: {job_id}")
            answer = input("   Force continue this job? (y/N): ").lower()
            if answer == 'y':
                force_continue_job(token, job_id)

if __name__ == "__main__":
    main()
