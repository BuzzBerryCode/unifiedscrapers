from fastapi import FastAPI
from datetime import datetime

# Create minimal FastAPI app for testing
app = FastAPI(title="Scraper Dashboard API", version="1.0.0")

@app.get("/")
async def root():
    return {"message": "Scraper Dashboard API", "version": "1.0.0", "status": "running"}

@app.get("/health")
async def health_check():
    """Health check endpoint for Railway"""
    return {
        "status": "healthy", 
        "timestamp": datetime.utcnow().isoformat(),
        "service": "Scraper Dashboard API"
    }

if __name__ == "__main__":
    import uvicorn
    import os
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
