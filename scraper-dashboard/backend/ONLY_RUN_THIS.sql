-- COPY ONLY THIS BLOCK - DO NOT COPY TABLE CREATION SCHEMA
-- Paste this into Supabase SQL Editor and run it

-- Step 1: Disable the trigger that auto-updates updated_at
ALTER TABLE creatordata DISABLE TRIGGER update_creatordata_updated_at;

-- Step 2: Update all creators with dates spread across past 6 days
WITH numbered_creators AS (
  SELECT 
    id,
    ROW_NUMBER() OVER (ORDER BY id) - 1 as row_num
  FROM creatordata
)
UPDATE creatordata 
SET updated_at = (
  CURRENT_TIMESTAMP 
  - INTERVAL '1 day' * (numbered_creators.row_num % 6 + 1)  -- Past 6 days (1-6 days ago)
  + INTERVAL '1 hour' * (8 + (numbered_creators.row_num * 3) % 10)  -- Random hours 8-17
  + INTERVAL '1 minute' * ((numbered_creators.row_num * 7) % 60)     -- Random minutes
)::timestamp with time zone
FROM numbered_creators
WHERE creatordata.id = numbered_creators.id;

-- Step 3: Re-enable the trigger
ALTER TABLE creatordata ENABLE TRIGGER update_creatordata_updated_at;

-- Step 4: Verify the results
SELECT 
  updated_at::date as updated_date,
  COUNT(*) as creators_count,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) as percentage,
  (updated_at::date + INTERVAL '7 days')::date as due_date
FROM creatordata 
WHERE updated_at IS NOT NULL
GROUP BY updated_at::date
ORDER BY updated_at::date;
