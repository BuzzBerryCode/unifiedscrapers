# 🚀 Deployment Guide

## 📋 Prerequisites

1. **Supabase Database** - Run the schema.sql to create required tables
2. **Redis Service** - For job queue (Upstash recommended for production)
3. **Railway Account** - For backend deployment
4. **Vercel Account** - For frontend deployment

## 🗄️ Step 1: Database Setup

1. Go to your Supabase dashboard
2. Open the SQL Editor
3. Copy and paste the contents of `backend/schema.sql`
4. Run the SQL to create the `scraper_jobs` table and triggers

## ☁️ Step 2: Backend Deployment (Railway)

1. **Create Railway Project**:
   ```bash
   # Install Railway CLI
   npm install -g @railway/cli
   
   # Login to Railway
   railway login
   
   # Create new project
   railway new
   ```

2. **Deploy Backend**:
   ```bash
   cd backend
   railway up
   ```

3. **Add Redis Service**:
   - Go to Railway dashboard
   - Click "New Service" → "Database" → "Redis"
   - Copy the Redis URL

4. **Set Environment Variables**:
   ```
   SUPABASE_URL=https://your-project.supabase.co
   SUPABASE_KEY=your-supabase-service-key
   REDIS_URL=redis://your-redis-url-from-railway
   JWT_SECRET_KEY=your-super-secret-jwt-key-change-this
   ADMIN_USERNAME=admin
   ADMIN_PASSWORD=your-secure-password
   SCRAPECREATORS_API_KEY=your-scrapecreators-api-key
   GEMINI_API_KEY=your-gemini-api-key
   ```

5. **Deploy Celery Worker**:
   - Create another Railway service for the worker
   - Use the same environment variables
   - Set start command to: `celery -A tasks worker --loglevel=info`

## 🌐 Step 3: Frontend Deployment (Vercel)

1. **Connect Repository**:
   - Go to Vercel dashboard
   - Click "New Project"
   - Import your GitHub repository
   - Set root directory to `scraper-dashboard/frontend`

2. **Set Environment Variables**:
   ```
   NEXT_PUBLIC_API_URL=https://your-backend-url.railway.app
   ```

3. **Deploy**:
   - Vercel will automatically build and deploy
   - Your dashboard will be available at `https://your-app.vercel.app`

## 🔧 Step 4: Testing

1. **Test Backend**:
   ```bash
   curl https://your-backend-url.railway.app/
   ```

2. **Test Frontend**:
   - Visit your Vercel URL
   - Login with admin credentials
   - Upload a test CSV file
   - Monitor job progress

## 📊 Step 5: Monitoring

### Backend Monitoring
- **Railway Logs**: Monitor API and worker logs
- **Redis Dashboard**: Monitor job queue status
- **Supabase Logs**: Monitor database operations

### Frontend Monitoring
- **Vercel Analytics**: Monitor frontend performance
- **Browser Console**: Check for JavaScript errors
- **Network Tab**: Monitor API calls

## 🛡️ Security Checklist

- [ ] Change default admin password
- [ ] Use strong JWT secret key
- [ ] Enable HTTPS for all services
- [ ] Restrict CORS origins to your domain
- [ ] Monitor API usage and rate limits
- [ ] Set up proper logging and alerting

## 🔄 CI/CD Pipeline (Optional)

Create `.github/workflows/deploy.yml`:
```yaml
name: Deploy
on:
  push:
    branches: [main]

jobs:
  deploy-backend:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Deploy to Railway
        uses: railway/cli@v1
        with:
          command: up
        env:
          RAILWAY_TOKEN: ${{ secrets.RAILWAY_TOKEN }}

  deploy-frontend:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Deploy to Vercel
        uses: vercel/action@v1
        with:
          vercel-token: ${{ secrets.VERCEL_TOKEN }}
          vercel-org-id: ${{ secrets.ORG_ID }}
          vercel-project-id: ${{ secrets.PROJECT_ID }}
```

## 🚨 Troubleshooting

### Common Issues

**Jobs stuck in pending:**
- Check Celery worker is running
- Verify Redis connection
- Check worker logs for errors

**API authentication errors:**
- Verify JWT secret key matches between frontend/backend
- Check token expiration (24 hours by default)
- Ensure admin credentials are correct

**File upload errors:**
- Verify CSV format (Usernames, Platform columns)
- Check file size limits
- Ensure backend has write permissions

**Database connection errors:**
- Verify Supabase URL and key
- Check database schema is created
- Ensure service key has proper permissions

### Support

For issues or questions:
1. Check Railway and Vercel logs
2. Monitor Redis queue status
3. Verify all environment variables
4. Test API endpoints manually

---

**🎉 Your scraper dashboard is now ready for production!**
