#!/usr/bin/env python3
import os
import sys
import traceback

def main():
    try:
        print("🚀 Starting Scraper Dashboard API...")
        print(f"Python version: {sys.version}")
        print(f"Current working directory: {os.getcwd()}")
        
        # List files in current directory for debugging
        try:
            files = [f for f in os.listdir('.') if f.endswith('.py')]
            print(f"📁 Python files in directory: {files}")
        except Exception as e:
            print(f"⚠️ Could not list files: {e}")
        
        # Get port from environment
        port = int(os.getenv("PORT", 8000))
        print(f"🌐 Port: {port}")
        
        # Test imports first
        print("🔍 Testing critical imports...")
        try:
            import fastapi
            print("✅ FastAPI import successful")
        except ImportError as e:
            print(f"❌ FastAPI import failed: {e}")
            sys.exit(1)
        
        try:
            import redis
            print("✅ Redis import successful")
        except ImportError as e:
            print(f"❌ Redis import failed: {e}")
            sys.exit(1)
        
        try:
            import supabase
            print("✅ Supabase import successful")
        except ImportError as e:
            print(f"❌ Supabase import failed: {e}")
            sys.exit(1)
        
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
