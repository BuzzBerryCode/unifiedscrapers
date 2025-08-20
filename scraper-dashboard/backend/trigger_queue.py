#!/usr/bin/env python3
"""
Trigger the queue system to start the pending job
"""

import os
import sys
from supabase import create_client, Client
from datetime import datetime

# Add current directory to path
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

# Configuration
SUPABASE_URL = os.getenv("SUPABASE_URL", "https://unovwhgnwenxbyvpevcz.supabase.co")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InVub3Z3aGdud2VueGJ5dnBldmN6Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1MjEzNjU0MCwiZXhwIjoyMDY3NzEyNTQwfQ.bqXWKTR64TRARH2VOrjiQdPnQ7W-8EGJp5RIi7yFmck")

def main():
    print("🚀 Triggering Queue System")
    print("=" * 40)
    
    # Connect to Supabase
    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        print("✅ Connected to Supabase")
    except Exception as e:
        print(f"❌ Supabase connection failed: {e}")
        return
    
    # Import the queue function
    try:
        from main import start_next_queued_job
        print("✅ Imported queue function")
        
        # Trigger the queue
        print("🔄 Starting next queued job...")
        start_next_queued_job()
        print("✅ Queue triggered successfully")
        
    except Exception as e:
        print(f"❌ Queue trigger failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
