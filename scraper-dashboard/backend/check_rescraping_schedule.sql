-- Monitor rescraping schedule and due creators

-- 1. Check creators due for rescraping (older than 7 days)
SELECT 
  'Creators due for rescraping (>7 days)' as metric,
  COUNT(*) as count
FROM creatordata 
WHERE updated_at < NOW() - INTERVAL '7 days';

-- 2. Breakdown by platform for due creators
SELECT 
  platform,
  COUNT(*) as due_count
FROM creatordata 
WHERE updated_at < NOW() - INTERVAL '7 days'
GROUP BY platform;

-- 3. Daily schedule for next 7 days (estimated)
WITH daily_schedule AS (
  SELECT 
    generate_series(
      CURRENT_DATE,
      CURRENT_DATE + INTERVAL '6 days',
      INTERVAL '1 day'
    )::date as schedule_date
),
due_by_day AS (
  SELECT 
    DATE(updated_at + INTERVAL '7 days') as due_date,
    platform,
    COUNT(*) as creators_count
  FROM creatordata 
  WHERE updated_at IS NOT NULL
    AND DATE(updated_at + INTERVAL '7 days') BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '6 days'
  GROUP BY DATE(updated_at + INTERVAL '7 days'), platform
)
SELECT 
  ds.schedule_date,
  TO_CHAR(ds.schedule_date, 'Day') as day_name,
  COALESCE(SUM(dbd.creators_count), 0) as estimated_rescrapers,
  COALESCE(SUM(CASE WHEN dbd.platform = 'instagram' THEN dbd.creators_count END), 0) as instagram_count,
  COALESCE(SUM(CASE WHEN dbd.platform = 'tiktok' THEN dbd.creators_count END), 0) as tiktok_count
FROM daily_schedule ds
LEFT JOIN due_by_day dbd ON ds.schedule_date = dbd.due_date
GROUP BY ds.schedule_date
ORDER BY ds.schedule_date;

-- 4. Show sample of creators due today
SELECT 
  handle,
  platform,
  primary_niche,
  updated_at,
  NOW() - updated_at as days_since_update
FROM creatordata 
WHERE updated_at < NOW() - INTERVAL '7 days'
ORDER BY updated_at ASC
LIMIT 20;

-- 5. Overall statistics
SELECT 
  COUNT(*) as total_creators,
  COUNT(CASE WHEN updated_at IS NULL THEN 1 END) as no_update_date,
  COUNT(CASE WHEN updated_at < NOW() - INTERVAL '7 days' THEN 1 END) as due_for_rescrape,
  COUNT(CASE WHEN updated_at >= NOW() - INTERVAL '7 days' THEN 1 END) as recently_updated,
  ROUND(AVG(EXTRACT(EPOCH FROM (NOW() - updated_at))/86400)::numeric, 1) as avg_days_since_update
FROM creatordata 
WHERE updated_at IS NOT NULL;
