#!/usr/bin/env python3
"""
Simple startup script for Railway
Bypasses all shell script issues
"""

import os
import sys

def main():
    print("🚀 DIRECT PYTHON STARTUP")
    print("=" * 40)
    print(f"Working directory: {os.getcwd()}")
    print(f"Python path: {sys.path}")
    
    # List files to verify we have everything
    files = [f for f in os.listdir('.') if f.endswith('.py')]
    print(f"Python files available: {files}")
    
    # Import and run the main application
    try:
        print("📦 Importing entrypoint...")
        from entrypoint import main as start_app
        print("✅ Entrypoint imported successfully")
        
        print("🚀 Starting application...")
        start_app()
        
    except Exception as e:
        print(f"❌ Startup failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
