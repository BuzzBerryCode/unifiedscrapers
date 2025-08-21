#!/usr/bin/env python3
"""
Test script to verify the deployment is working correctly
"""

import requests
import json

def test_render_deployment():
    """Test the Render deployment endpoints"""
    base_url = "https://scraper-backend-zvy9.onrender.com"
    
    print("🧪 Testing Render Deployment")
    print("=" * 50)
    
    # Test 1: Health check
    try:
        print("1. Testing health endpoint...")
        response = requests.get(f"{base_url}/health", timeout=30)
        print(f"   Status: {response.status_code}")
        if response.status_code == 200:
            health_data = response.json()
            print(f"   ✅ Service: {health_data.get('status', 'unknown')}")
            print(f"   ✅ Redis: {health_data.get('redis_connected', False)}")
            print(f"   ✅ Supabase: {health_data.get('supabase_connected', False)}")
        else:
            print(f"   ❌ Health check failed: {response.text}")
    except Exception as e:
        print(f"   ❌ Health check error: {e}")
    
    # Test 2: Login endpoint
    try:
        print("\n2. Testing login endpoint...")
        login_data = {"username": "admin", "password": "buzzberry2024"}
        response = requests.post(f"{base_url}/auth/login", json=login_data, timeout=30)
        print(f"   Status: {response.status_code}")
        if response.status_code == 200:
            token_data = response.json()
            token = token_data.get("access_token")
            print(f"   ✅ Login successful, token received")
            
            # Test 3: Stats endpoint (requires auth)
            print("\n3. Testing stats endpoint...")
            headers = {"Authorization": f"Bearer {token}"}
            response = requests.get(f"{base_url}/stats", headers=headers, timeout=30)
            print(f"   Status: {response.status_code}")
            if response.status_code == 200:
                stats = response.json()
                print(f"   ✅ Total creators: {stats.get('total_creators', 0)}")
                print(f"   ✅ Instagram: {stats.get('instagram_creators', 0)}")
                print(f"   ✅ TikTok: {stats.get('tiktok_creators', 0)}")
            else:
                print(f"   ❌ Stats failed: {response.text}")
            
            # Test 4: Jobs endpoint
            print("\n4. Testing jobs endpoint...")
            response = requests.get(f"{base_url}/jobs", headers=headers, timeout=30)
            print(f"   Status: {response.status_code}")
            if response.status_code == 200:
                jobs = response.json()
                print(f"   ✅ Jobs retrieved: {len(jobs)} jobs found")
            else:
                print(f"   ❌ Jobs failed: {response.text}")
                
        else:
            print(f"   ❌ Login failed: {response.text}")
    except Exception as e:
        print(f"   ❌ Login error: {e}")
    
    print("\n" + "=" * 50)
    print("🏁 Deployment test complete!")

if __name__ == "__main__":
    test_render_deployment()
