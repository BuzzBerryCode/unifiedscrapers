-- Advanced: Populate updated_at dates with even distribution across days AND platforms
-- This ensures both Instagram and TikTok creators are spread evenly throughout the week

-- Check current status
SELECT 
  platform,
  COUNT(*) as total_creators,
  COUNT(CASE WHEN updated_at IS NULL THEN 1 END) as need_dates,
  COUNT(CASE WHEN updated_at IS NOT NULL THEN 1 END) as have_dates
FROM creatordata 
GROUP BY platform;

-- Update with platform-aware distribution
WITH creator_ranking AS (
  SELECT 
    id,
    platform,
    -- Rank creators within each platform
    ROW_NUMBER() OVER (PARTITION BY platform ORDER BY id) as platform_rank,
    -- Total creators per platform that need dates
    COUNT(*) OVER (PARTITION BY platform) as platform_total
  FROM creatordata 
  WHERE updated_at IS NULL
),
date_assignment AS (
  SELECT 
    id,
    platform,
    platform_rank,
    -- Assign day of week (0-6) based on platform rank
    (platform_rank - 1) % 7 as day_offset,
    -- Calculate base date (7 days ago)
    NOW() - INTERVAL '7 days' as base_date
  FROM creator_ranking
)
UPDATE creatordata 
SET updated_at = (
  da.base_date +
  -- Add day offset
  INTERVAL '1 day' * da.day_offset +
  -- Add random hour within the day (spread throughout 24 hours)
  INTERVAL '1 hour' * FLOOR(RANDOM() * 24) +
  -- Add random minutes
  INTERVAL '1 minute' * FLOOR(RANDOM() * 60) +
  -- Add random seconds for more precision
  INTERVAL '1 second' * FLOOR(RANDOM() * 60)
)
FROM date_assignment da
WHERE creatordata.id = da.id;

-- Verify the distribution by day and platform
SELECT 
  DATE(updated_at) as update_date,
  platform,
  COUNT(*) as creators_count
FROM creatordata 
WHERE updated_at >= NOW() - INTERVAL '8 days'
GROUP BY DATE(updated_at), platform
ORDER BY update_date, platform;

-- Summary by day (should be roughly even)
SELECT 
  DATE(updated_at) as update_date,
  COUNT(*) as total_creators,
  COUNT(CASE WHEN platform = 'instagram' THEN 1 END) as instagram_count,
  COUNT(CASE WHEN platform = 'tiktok' THEN 1 END) as tiktok_count
FROM creatordata 
WHERE updated_at >= NOW() - INTERVAL '8 days'
GROUP BY DATE(updated_at)
ORDER BY update_date;

-- Final verification
SELECT 
  'Total creators' as metric,
  COUNT(*) as count
FROM creatordata
UNION ALL
SELECT 
  'Still need dates' as metric,
  COUNT(*) as count
FROM creatordata 
WHERE updated_at IS NULL
UNION ALL
SELECT 
  'Have dates' as metric,
  COUNT(*) as count
FROM creatordata 
WHERE updated_at IS NOT NULL;
