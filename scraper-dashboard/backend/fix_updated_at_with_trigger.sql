-- FIX UPDATED_AT DISTRIBUTION - Working around the trigger
-- The trigger "update_creatordata_updated_at" automatically sets updated_at to NOW()
-- We need to either disable the trigger temporarily or use a different approach

-- OPTION 1: Disable trigger, update, re-enable trigger
-- This is the cleanest approach

-- Step 1: Disable the trigger temporarily
ALTER TABLE creatordata DISABLE TRIGGER update_creatordata_updated_at;

-- Step 2: Update all creators with staggered dates across the past 6 days
UPDATE creatordata 
SET updated_at = (
  -- Spread across past 6 days (not 7, as requested)
  CURRENT_TIMESTAMP 
  - INTERVAL '1 day' * ((ROW_NUMBER() OVER (ORDER BY id) - 1) % 6 + 1)
  + INTERVAL '1 hour' * (6 + ((ROW_NUMBER() OVER (ORDER BY id) - 1) * 7) % 13)
  + INTERVAL '1 minute' * (((ROW_NUMBER() OVER (ORDER BY id) - 1) * 17) % 60)
)::timestamp with time zone;

-- Step 3: Re-enable the trigger
ALTER TABLE creatordata ENABLE TRIGGER update_creatordata_updated_at;

-- Step 4: Verify the distribution
SELECT 
  updated_at::date as updated_date,
  TO_CHAR(updated_at::date, 'Day') as day_name,
  COUNT(*) as creators_count,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) as percentage,
  -- Show when these creators will be due for rescraping (7 days later)
  (updated_at::date + INTERVAL '7 days')::date as due_date,
  TO_CHAR((updated_at::date + INTERVAL '7 days')::date, 'Day') as due_day_name
FROM creatordata 
WHERE updated_at IS NOT NULL
GROUP BY updated_at::date, TO_CHAR(updated_at::date, 'Day')
ORDER BY updated_at::date;

-- Step 5: Summary
SELECT 
  'DISTRIBUTION SUMMARY' as info,
  COUNT(*) as total_creators,
  COUNT(DISTINCT updated_at::date) as unique_dates,
  MIN(updated_at::date) as earliest_updated,
  MAX(updated_at::date) as latest_updated,
  -- Show the range of due dates (7 days after updated_at)
  MIN((updated_at + INTERVAL '7 days')::date) as first_due_date,
  MAX((updated_at + INTERVAL '7 days')::date) as last_due_date
FROM creatordata 
WHERE updated_at IS NOT NULL;
