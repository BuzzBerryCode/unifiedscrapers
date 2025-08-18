-- Create scraper_jobs table for job management
CREATE TABLE IF NOT EXISTS scraper_jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_type VARCHAR(50) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    description TEXT,
    total_items INTEGER DEFAULT 0,
    processed_items INTEGER DEFAULT 0,
    failed_items INTEGER DEFAULT 0,
    results JSONB,
    error_message TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create index on status for faster queries
CREATE INDEX IF NOT EXISTS idx_scraper_jobs_status ON scraper_jobs(status);

-- Create index on created_at for sorting
CREATE INDEX IF NOT EXISTS idx_scraper_jobs_created_at ON scraper_jobs(created_at DESC);

-- Create index on job_type for filtering
CREATE INDEX IF NOT EXISTS idx_scraper_jobs_type ON scraper_jobs(job_type);

-- Add updated_at column to creatordata table if it doesn't exist
DO $$ 
BEGIN 
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name='creatordata' AND column_name='updated_at') THEN
        ALTER TABLE creatordata ADD COLUMN updated_at TIMESTAMP WITH TIME ZONE;
    END IF;
END $$;

-- Create index on updated_at for performance
CREATE INDEX IF NOT EXISTS idx_creatordata_updated_at ON creatordata(updated_at);

-- Create function to automatically update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create trigger to automatically update updated_at on scraper_jobs
DROP TRIGGER IF EXISTS update_scraper_jobs_updated_at ON scraper_jobs;
CREATE TRIGGER update_scraper_jobs_updated_at
    BEFORE UPDATE ON scraper_jobs
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Create trigger to automatically update updated_at on creatordata
DROP TRIGGER IF EXISTS update_creatordata_updated_at ON creatordata;
CREATE TRIGGER update_creatordata_updated_at
    BEFORE UPDATE ON creatordata
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Create admin user table (optional - for future use)
CREATE TABLE IF NOT EXISTS admin_users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    username VARCHAR(50) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    email VARCHAR(255),
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    last_login TIMESTAMP WITH TIME ZONE
);

-- Insert default admin user (password: scraper123)
-- Note: In production, change this password immediately
INSERT INTO admin_users (username, password_hash, email) 
VALUES (
    'admin', 
    '$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LewdBPj/3UjKyZrDy', -- scraper123
    'admin@buzzberrycode.com'
) ON CONFLICT (username) DO NOTHING;

-- Add comments for documentation
COMMENT ON TABLE scraper_jobs IS 'Stores information about scraping jobs and their status';
COMMENT ON COLUMN scraper_jobs.job_type IS 'Type of job: new_creators, rescrape_all, rescrape_platform';
COMMENT ON COLUMN scraper_jobs.status IS 'Job status: pending, running, completed, failed, cancelled';
COMMENT ON COLUMN scraper_jobs.results IS 'JSON object containing job results and statistics';

COMMENT ON TABLE admin_users IS 'Admin users who can access the dashboard';
COMMENT ON COLUMN creatordata.updated_at IS 'Timestamp when the creator record was last updated';
