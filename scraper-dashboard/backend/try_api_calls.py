#!/usr/bin/env python3
"""
Try different API URLs to find the correct one and manage jobs
"""

import requests
import json
import sys

# Possible API URLs to try
POSSIBLE_URLS = [
    "https://scraper-dashboard-backend-production.up.railway.app",
    "https://unified-scrapers-backend-production.up.railway.app", 
    "https://backend-production.up.railway.app",
    "https://scraper-backend-production.up.railway.app",
    "https://creatorscraper-backend.up.railway.app"
]

USERNAME = "admin"
PASSWORD = "buzzberry123"

def test_url(api_url):
    """Test if an API URL works"""
    try:
        print(f"🔍 Testing: {api_url}")
        
        # Try to login
        response = requests.post(f"{api_url}/login", 
            json={"username": USERNAME, "password": PASSWORD},
            timeout=10
        )
        
        if response.status_code == 200:
            token = response.json()["access_token"]
            print(f"✅ Login successful at {api_url}")
            
            # Get running jobs
            jobs_response = requests.get(f"{api_url}/jobs/running",
                headers={"Authorization": f"Bearer {token}"},
                timeout=10
            )
            
            if jobs_response.status_code == 200:
                jobs_data = jobs_response.json()
                print(f"📋 Found {jobs_data['count']} running jobs")
                
                rescraper_job_id = None
                new_creators_job_id = None
                
                for job in jobs_data['running_jobs']:
                    print(f"   Job: {job['id'][:8]}... - {job['description']}")
                    
                    if 'rescrape' in job['description'].lower() and 'instagram' in job['description'].lower():
                        rescraper_job_id = job['id']
                        print(f"   🎯 RESCRAPER JOB ID: {rescraper_job_id}")
                    
                    if 'crypto' in job['description'].lower() and 'trading' in job['description'].lower():
                        new_creators_job_id = job['id']
                        print(f"   🎯 NEW CREATORS JOB ID: {new_creators_job_id}")
                
                # Cancel rescraper if found
                if rescraper_job_id:
                    print(f"\n❌ Cancelling rescraper job...")
                    cancel_response = requests.delete(f"{api_url}/jobs/{rescraper_job_id}",
                        headers={"Authorization": f"Bearer {token}"},
                        timeout=10
                    )
                    if cancel_response.status_code == 200:
                        print("✅ Rescraper job cancelled successfully")
                    else:
                        print(f"❌ Failed to cancel rescraper: {cancel_response.status_code}")
                        print(cancel_response.text)
                
                # Force continue new creators job if found
                if new_creators_job_id:
                    print(f"\n🚀 Force continuing new creators job...")
                    continue_response = requests.post(f"{api_url}/jobs/{new_creators_job_id}/force-continue",
                        headers={"Authorization": f"Bearer {token}"},
                        timeout=10
                    )
                    if continue_response.status_code == 200:
                        result = continue_response.json()
                        print(f"✅ New creators job force continued from index {result['resume_from_index']}")
                    else:
                        print(f"❌ Failed to force continue: {continue_response.status_code}")
                        print(continue_response.text)
                
                return True
            else:
                print(f"❌ Failed to get jobs: {jobs_response.status_code}")
                return False
        else:
            print(f"❌ Login failed: {response.status_code}")
            return False
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Connection failed: {e}")
        return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def main():
    print("🔧 Trying to find the correct API URL and manage jobs...")
    print("=" * 60)
    
    for url in POSSIBLE_URLS:
        if test_url(url):
            print(f"\n🎉 SUCCESS! Used API URL: {url}")
            return
        print()
    
    print("❌ None of the API URLs worked. Please check:")
    print("1. Railway deployment is running")
    print("2. Environment variables are set correctly")
    print("3. The actual Railway URL")

if __name__ == "__main__":
    main()
