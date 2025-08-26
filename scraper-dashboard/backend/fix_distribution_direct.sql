-- DIRECT SQL SOLUTION: Fix Rescraping Distribution
-- This is MUCH more efficient than processing through the backend API
-- Run this directly in your Supabase SQL editor

-- Step 1: Create a sequence for all creators and assign them to days
WITH creator_sequence AS (
  SELECT 
    id,
    handle,
    platform,
    ROW_NUMBER() OVER (ORDER BY id) - 1 as seq_num
  FROM creatordata
),

-- Step 2: Calculate the updated_at date for each creator
-- Spread across the PAST 7 days so they become due on DIFFERENT days
date_assignments AS (
  SELECT 
    id,
    handle,
    platform,
    seq_num,
    seq_num % 7 as day_offset,
    -- Calculate days_ago: day_offset=0 -> 7 days ago, day_offset=1 -> 6 days ago, etc.
    (7 - (seq_num % 7)) as days_ago,
    -- Calculate the target updated_at date (spread across past 7 days)
    (CURRENT_TIMESTAMP - INTERVAL '1 day' * (7 - (seq_num % 7)) 
     + INTERVAL '1 hour' * (6 + (seq_num * 7) % 13)  -- Random hours 6-18
     + INTERVAL '1 minute' * ((seq_num * 17) % 60)    -- Random minutes 0-59
     + INTERVAL '1 second' * ((seq_num * 23) % 60)    -- Random seconds 0-59
    )::timestamp as new_updated_at
  FROM creator_sequence
)

-- Step 3: Update all creators with their new staggered updated_at dates
UPDATE creatordata 
SET updated_at = date_assignments.new_updated_at
FROM date_assignments 
WHERE creatordata.id = date_assignments.id;

-- Step 4: Verification - Show the distribution of due dates (7 days after updated_at)
SELECT 
  due_date,
  TO_CHAR(due_date, 'Day') as day_name,
  COUNT(*) as creators_due,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) as percentage
FROM (
  SELECT 
    (updated_at + INTERVAL '7 days')::date as due_date
  FROM creatordata 
  WHERE updated_at IS NOT NULL
) due_schedule
GROUP BY due_date, TO_CHAR(due_date, 'Day')
ORDER BY due_date;

-- Step 5: Summary statistics
SELECT 
  'SUMMARY' as info,
  COUNT(*) as total_creators,
  COUNT(CASE WHEN updated_at IS NULL THEN 1 END) as null_dates,
  COUNT(CASE WHEN updated_at IS NOT NULL THEN 1 END) as with_dates,
  MIN(updated_at::date) as earliest_date,
  MAX(updated_at::date) as latest_date
FROM creatordata;
