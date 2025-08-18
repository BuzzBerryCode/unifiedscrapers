# 🚀 Scraper Dashboard

A web-based management system for the Unified Scrapers with real-time job monitoring, CSV uploads, and admin dashboard.

## 🏗️ Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Frontend      │    │    Backend      │    │   Job Queue     │
│   (Vercel)      │◄──►│  (Railway)      │◄──►│   (Upstash)     │
│                 │    │                 │    │                 │
│ • Next.js       │    │ • FastAPI       │    │ • Redis         │
│ • Tailwind CSS  │    │ • Authentication│    │ • Celery        │
│ • File Upload   │    │ • CSV Processing│    │ • Job Status    │
│ • Job Dashboard │    │ • Scraper Exec  │    │ • Queue Mgmt    │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         │                       │                       │
         └───────────────────────▼───────────────────────┘
                        ┌─────────────────┐
                        │   Supabase      │
                        │                 │
                        │ • Creator Data  │
                        │ • Job History   │
                        │ • Media Storage │
                        │ • User Auth     │
                        └─────────────────┘
```

## ✨ Features

### 🎯 **Frontend Dashboard**
- **Admin Authentication** - Secure login system
- **CSV Upload** - Drag & drop interface for new creators
- **Job Management** - Create, monitor, and cancel jobs
- **Real-time Updates** - Live job status and progress
- **Statistics** - Creator counts and job metrics
- **Responsive Design** - Works on desktop and mobile

### 🔧 **Backend API**
- **RESTful API** - FastAPI with automatic documentation
- **Job Queue** - Redis + Celery for background processing
- **Authentication** - JWT-based admin authentication
- **File Processing** - CSV parsing and validation
- **Database Integration** - Supabase for data storage
- **Error Handling** - Comprehensive error management

### 📊 **Job Types**
1. **New Creators** - Process creators from CSV upload
2. **Rescrape All** - Rescrape all existing creators
3. **Rescrape Platform** - Rescrape Instagram or TikTok creators only

## 🚀 Quick Start

### Prerequisites
- Node.js 18+ (for frontend)
- Python 3.11+ (for backend)
- Redis (for job queue)
- Supabase account (for database)

### 1. Database Setup

Run the schema in your Supabase database:
```sql
-- Copy and run the contents of backend/schema.sql
```

### 2. Backend Setup

```bash
cd backend

# Install dependencies
pip install -r requirements.txt

# Copy environment variables
cp env.example .env
# Edit .env with your actual values

# Run the API server
uvicorn main:app --reload --host 0.0.0.0 --port 8000

# In another terminal, run the Celery worker
celery -A tasks worker --loglevel=info
```

### 3. Frontend Setup

```bash
cd frontend

# Install dependencies
npm install

# Copy environment variables
cp env.local.example .env.local
# Edit .env.local with your backend URL

# Run the development server
npm run dev
```

### 4. Access the Dashboard

Open http://localhost:3000 in your browser and login with:
- **Username**: admin
- **Password**: scraper123

## 🌐 Production Deployment

### Backend (Railway)

1. Create a new Railway project
2. Connect your GitHub repository
3. Set environment variables in Railway dashboard
4. Deploy the backend folder
5. Add Redis service to your Railway project

### Frontend (Vercel)

1. Connect your GitHub repository to Vercel
2. Set the root directory to `scraper-dashboard/frontend`
3. Set environment variables in Vercel dashboard
4. Deploy

### Environment Variables

**Backend (.env):**
```
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_KEY=your-supabase-service-key
REDIS_URL=redis://your-redis-url
JWT_SECRET_KEY=your-super-secret-jwt-key
ADMIN_USERNAME=admin
ADMIN_PASSWORD=your-secure-password
SCRAPECREATORS_API_KEY=your-scrapecreators-api-key
GEMINI_API_KEY=your-gemini-api-key
```

**Frontend (.env.local):**
```
NEXT_PUBLIC_API_URL=https://your-backend-url.railway.app
```

## 📋 Usage

### Upload New Creators
1. Prepare CSV file with columns: `Usernames`, `Platform`
2. Drag & drop or select the CSV file
3. Click "Upload & Process"
4. Monitor job progress in real-time

### Rescrape Existing Creators
1. Click "Rescrape All" for all creators
2. Click "Rescrape Instagram" for Instagram only
3. Click "Rescrape TikTok" for TikTok only
4. Monitor progress and results

### Monitor Jobs
- View all jobs in the dashboard table
- Click the eye icon to see detailed results
- Cancel running jobs with the trash icon
- Real-time progress updates every 5 seconds

## 🔧 Configuration

### Job Queue Settings
```python
# Backend configuration
CLEANUP_BATCH_SIZE = 2      # Concurrent creators
BATCH_DELAY = 2.0          # Delay between batches
```

### Frontend Polling
```javascript
// Auto-refresh every 5 seconds
const interval = setInterval(fetchData, 5000)
```

## 🛡️ Security

- **JWT Authentication** - Secure token-based auth
- **CORS Protection** - Configured for frontend domains
- **Input Validation** - CSV and request validation
- **Environment Variables** - Sensitive data in env vars
- **Rate Limiting** - API rate limiting and delays

## 📊 API Documentation

Once the backend is running, visit:
- **Interactive Docs**: http://localhost:8000/docs
- **OpenAPI Schema**: http://localhost:8000/openapi.json

## 🔍 Troubleshooting

### Common Issues

**Backend won't start:**
- Check Redis connection
- Verify environment variables
- Ensure all dependencies are installed

**Jobs stuck in pending:**
- Check Celery worker is running
- Verify Redis connection
- Check backend logs

**Frontend can't connect:**
- Verify API URL in .env.local
- Check CORS settings in backend
- Ensure backend is running

### Logs

**Backend logs:**
```bash
# API server logs
uvicorn main:app --log-level debug

# Celery worker logs  
celery -A tasks worker --loglevel=debug
```

**Frontend logs:**
```bash
# Development server
npm run dev

# Build logs
npm run build
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## 📄 License

This project is proprietary software owned by BuzzBerry Code.

---

**Built with ❤️ by BuzzBerry Code**
