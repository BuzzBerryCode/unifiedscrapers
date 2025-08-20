#!/usr/bin/env python3
"""
Railway Entry Point - Redirects to actual application
"""

import sys
import os

# Add the backend directory to Python path
backend_dir = os.path.join(os.path.dirname(__file__), 'scraper-dashboard', 'backend')
sys.path.insert(0, backend_dir)

# Change working directory to backend
os.chdir(backend_dir)

# Import and run the actual application
if __name__ == "__main__":
    from main import app
    import uvicorn
    
    port = int(os.getenv("PORT", 8000))
    print(f"🚀 Starting from repository root, redirecting to backend...")
    print(f"📁 Backend directory: {backend_dir}")
    print(f"🌐 Port: {port}")
    
    uvicorn.run(app, host="0.0.0.0", port=port)
