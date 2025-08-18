#!/usr/bin/env python3
import os
import sys
import traceback
import time

def test_imports():
    """Test all imports to see what's failing"""
    print("🔍 Testing imports...")
    
    try:
        print("  ✓ Testing basic imports...")
        import json
        import datetime
        print("  ✓ Basic imports OK")
        
        print("  ✓ Testing FastAPI...")
        from fastapi import FastAPI
        print("  ✓ FastAPI OK")
        
        print("  ✓ Testing uvicorn...")
        import uvicorn
        print("  ✓ Uvicorn OK")
        
        print("  ✓ Testing pandas...")
        import pandas
        print("  ✓ Pandas OK")
        
        print("  ✓ Testing supabase...")
        from supabase import create_client
        print("  ✓ Supabase OK")
        
        print("  ✓ Testing redis...")
        import redis
        print("  ✓ Redis OK")
        
        print("  ✓ Testing celery...")
        from celery import Celery
        print("  ✓ Celery OK")
        
        print("  ✓ All imports successful!")
        return True
        
    except Exception as e:
        print(f"  ❌ Import failed: {e}")
        traceback.print_exc()
        return False

def test_minimal_app():
    """Test a minimal FastAPI app"""
    try:
        print("🚀 Testing minimal FastAPI app...")
        from fastapi import FastAPI
        app = FastAPI()
        
        @app.get("/health")
        def health():
            return {"status": "healthy"}
            
        print("  ✓ Minimal app created successfully")
        return app
    except Exception as e:
        print(f"  ❌ Minimal app failed: {e}")
        traceback.print_exc()
        return None

def main():
    print("🔧 DEBUGGING RAILWAY DEPLOYMENT")
    print(f"Python version: {sys.version}")
    print(f"Working directory: {os.getcwd()}")
    print(f"Environment variables:")
    for key in ['PORT', 'SUPABASE_URL', 'REDIS_URL']:
        print(f"  {key}: {os.getenv(key, 'NOT SET')}")
    
    # Test imports
    if not test_imports():
        print("❌ Import test failed - cannot proceed")
        sys.exit(1)
    
    # Test minimal app
    app = test_minimal_app()
    if not app:
        print("❌ Minimal app test failed")
        sys.exit(1)
    
    # Try to start server
    try:
        port_str = os.getenv("PORT", "8000")
        print(f"🌐 PORT environment variable: '{port_str}'")
        
        try:
            port = int(port_str)
            print(f"🌐 Parsed port: {port}")
        except ValueError as e:
            print(f"❌ Invalid port value: {port_str}, using default 8000")
            port = 8000
        
        print(f"🌐 Starting server on port {port}...")
        
        import uvicorn
        uvicorn.run(
            app,
            host="0.0.0.0",
            port=port,
            log_level="info"
        )
        
    except Exception as e:
        print(f"❌ Server startup failed: {e}")
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
