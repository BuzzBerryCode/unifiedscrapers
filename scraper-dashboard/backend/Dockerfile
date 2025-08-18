FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libffi-dev \
    libssl-dev \
    python3-dev \
    build-essential \
    pkg-config \
    libheif-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Make start script executable
RUN chmod +x start.sh

# Expose port
EXPOSE 8000

# Command to run the application - use start script if available, fallback to direct uvicorn
CMD ["sh", "-c", "if [ -f start.sh ]; then ./start.sh; else uvicorn main:app --host 0.0.0.0 --port ${PORT:-8000}; fi"]
