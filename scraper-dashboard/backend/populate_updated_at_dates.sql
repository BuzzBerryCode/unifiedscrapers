-- Populate null updated_at dates for creators, spread across the past 7 days
-- This prevents all creators from being rescraped at once

-- First, let's see how many creators need dates
SELECT COUNT(*) as creators_needing_dates 
FROM creatordata 
WHERE updated_at IS NULL;

-- Update creators with null updated_at dates
-- Spread them across the past 7 days with random times
UPDATE creatordata 
SET updated_at = (
  -- Base date: 7 days ago
  NOW() - INTERVAL '7 days' +
  -- Add day offset based on row number (0-6 days)
  INTERVAL '1 day' * (ROW_NUMBER() OVER (ORDER BY id) % 7) +
  -- Add random hours (0-23)
  INTERVAL '1 hour' * FLOOR(RANDOM() * 24) +
  -- Add random minutes (0-59)
  INTERVAL '1 minute' * FLOOR(RANDOM() * 60)
)
WHERE updated_at IS NULL;

-- Verify the update worked
SELECT 
  DATE(updated_at) as update_date,
  COUNT(*) as creators_count
FROM creatordata 
WHERE updated_at >= NOW() - INTERVAL '8 days'
GROUP BY DATE(updated_at)
ORDER BY update_date;

-- Show total counts
SELECT 
  COUNT(*) as total_creators,
  COUNT(CASE WHEN updated_at IS NULL THEN 1 END) as still_null,
  COUNT(CASE WHEN updated_at IS NOT NULL THEN 1 END) as has_dates
FROM creatordata;
