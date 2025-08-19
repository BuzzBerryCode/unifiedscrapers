#!/bin/bash

# Job Management Script
# This script will help cancel the rescraper and force continue the new creators job

echo "🔧 Job Management Script"
echo "========================"

# Configuration - Update these with your actual values
API_URL="https://scraper-dashboard-backend-production.up.railway.app"
USERNAME="admin"
PASSWORD="buzzberry123"

echo "🔑 Getting authentication token..."

# Login and get token
TOKEN_RESPONSE=$(curl -s -X POST "$API_URL/login" \
  -H "Content-Type: application/json" \
  -d "{\"username\":\"$USERNAME\",\"password\":\"$PASSWORD\"}")

if [[ $TOKEN_RESPONSE == *"access_token"* ]]; then
    TOKEN=$(echo $TOKEN_RESPONSE | grep -o '"access_token":"[^"]*' | grep -o '[^"]*$')
    echo "✅ Login successful"
else
    echo "❌ Login failed:"
    echo $TOKEN_RESPONSE
    exit 1
fi

echo ""
echo "📋 Getting running jobs..."

# Get running jobs
RUNNING_JOBS=$(curl -s -X GET "$API_URL/jobs/running" \
  -H "Authorization: Bearer $TOKEN")

echo "Running jobs response:"
echo $RUNNING_JOBS | python3 -m json.tool 2>/dev/null || echo $RUNNING_JOBS

echo ""
echo "🎯 Looking for jobs to manage..."

# Extract job IDs (this is a simplified approach)
# You'll need to manually identify the job IDs from the output above

echo ""
echo "📝 MANUAL STEPS:"
echo "1. Look at the running jobs output above"
echo "2. Find the job ID for 'Rescrape all 552 Instagram creators'"
echo "3. Find the job ID for 'Process 507 creators from Crypto + Trading V.1.csv'"
echo ""
echo "Then run these commands:"
echo ""
echo "# Cancel rescraper job (replace JOB_ID):"
echo "curl -X DELETE \"$API_URL/jobs/RESCRAPER_JOB_ID\" -H \"Authorization: Bearer $TOKEN\""
echo ""
echo "# Force continue new creators job (replace JOB_ID):"
echo "curl -X POST \"$API_URL/jobs/NEW_CREATORS_JOB_ID/force-continue\" -H \"Authorization: Bearer $TOKEN\""
echo ""
echo "Your auth token is: $TOKEN"
