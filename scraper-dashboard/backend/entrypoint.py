#!/usr/bin/env python3
import os
import sys
import traceback

def main():
    try:
        print("🚀 Starting Scraper Dashboard API...")
        print(f"Python version: {sys.version}")
        print(f"Current working directory: {os.getcwd()}")
        
        # Get port from environment
        port = int(os.getenv("PORT", 8000))
        print(f"🌐 Port: {port}")
        
        # Import and start the app
        print("📦 Importing FastAPI app...")
        from main import app
        print("✅ FastAPI app imported successfully")
        
        print("🔥 Starting uvicorn server...")
        import uvicorn
        uvicorn.run(
            app, 
            host="0.0.0.0", 
            port=port,
            log_level="info",
            access_log=True
        )
        
    except Exception as e:
        print(f"❌ Error starting server: {e}")
        print(f"📋 Traceback:")
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
