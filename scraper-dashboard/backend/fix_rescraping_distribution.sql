-- Fix Rescraping Distribution: Spread all creators evenly across 7 days
-- This ensures equal distribution for rescraping workload

-- Step 1: Create a temporary sequence to assign creators to days
WITH creator_sequence AS (
  SELECT 
    id,
    handle,
    platform,
    ROW_NUMBER() OVER (ORDER BY id) - 1 as seq_num
  FROM creatordata
),

-- Step 2: Calculate which day each creator should be assigned to (0-6 for 7 days)
day_assignments AS (
  SELECT 
    id,
    handle,
    platform,
    seq_num,
    seq_num % 7 as day_offset  -- This ensures even distribution across 7 days
  FROM creator_sequence
),

-- Step 3: Calculate the actual updated_at date for each creator
-- Start from 8 days ago and add the day_offset to spread across 7 days
date_assignments AS (
  SELECT 
    id,
    handle,
    platform,
    day_offset,
    -- Start from 8 days ago at 9:00 AM UTC, then add day_offset
    (CURRENT_TIMESTAMP - INTERVAL '8 days' + INTERVAL '1 day' * day_offset)::timestamp as new_updated_at
  FROM day_assignments
)

-- Step 4: Update all creators with their new evenly distributed dates
UPDATE creatordata 
SET updated_at = date_assignments.new_updated_at
FROM date_assignments 
WHERE creatordata.id = date_assignments.id;

-- Verification: Show the distribution after update
SELECT 
  DATE(updated_at) as update_date,
  EXTRACT(DOW FROM updated_at) as day_of_week,
  TO_CHAR(updated_at, 'Day') as day_name,
  COUNT(*) as creator_count
FROM creatordata 
WHERE updated_at IS NOT NULL
GROUP BY DATE(updated_at), EXTRACT(DOW FROM updated_at), TO_CHAR(updated_at, 'Day')
ORDER BY DATE(updated_at);

-- Summary: Show total counts
SELECT 
  COUNT(*) as total_creators,
  COUNT(CASE WHEN updated_at IS NULL THEN 1 END) as null_dates,
  COUNT(CASE WHEN updated_at IS NOT NULL THEN 1 END) as with_dates
FROM creatordata;
