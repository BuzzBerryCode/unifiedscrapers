-- CORRECT Distribution Logic for Rescraping Schedule
-- Goal: Spread creators across PAST 7 days so they become due on DIFFERENT days

-- The key insight:
-- - A creator is due for rescraping 7 days AFTER their updated_at date
-- - To have creators due on different days, they need different updated_at dates
-- - If we want even distribution of due dates, we need even distribution of updated_at dates

-- Example with 14 creators:
-- Creator 0: updated_at = 7 days ago → due TODAY (7 days after updated_at)
-- Creator 1: updated_at = 6 days ago → due TOMORROW (7 days after updated_at)  
-- Creator 2: updated_at = 5 days ago → due in 2 days (7 days after updated_at)
-- Creator 3: updated_at = 4 days ago → due in 3 days (7 days after updated_at)
-- Creator 4: updated_at = 3 days ago → due in 4 days (7 days after updated_at)
-- Creator 5: updated_at = 2 days ago → due in 5 days (7 days after updated_at)
-- Creator 6: updated_at = 1 day ago → due in 6 days (7 days after updated_at)
-- Creator 7: updated_at = 7 days ago → due TODAY (cycle repeats)
-- Creator 8: updated_at = 6 days ago → due TOMORROW
-- ... and so on

-- SQL Implementation:
WITH creator_sequence AS (
  SELECT 
    id,
    handle,
    platform,
    ROW_NUMBER() OVER (ORDER BY id) - 1 as seq_num
  FROM creatordata
),

date_assignments AS (
  SELECT 
    id,
    handle,
    platform,
    seq_num,
    seq_num % 7 as day_offset,
    -- Calculate days_ago: day_offset=0 -> 7 days ago, day_offset=1 -> 6 days ago, etc.
    (7 - (seq_num % 7)) as days_ago,
    -- Calculate the updated_at date
    (CURRENT_TIMESTAMP - INTERVAL '1 day' * (7 - (seq_num % 7)))::timestamp as new_updated_at
  FROM creator_sequence
),

-- Show when each creator will be due (7 days after updated_at)
due_dates AS (
  SELECT 
    *,
    (new_updated_at + INTERVAL '7 days')::date as due_date
  FROM date_assignments
)

-- Update all creators with staggered updated_at dates
UPDATE creatordata 
SET updated_at = date_assignments.new_updated_at
FROM date_assignments 
WHERE creatordata.id = date_assignments.id;

-- Verification: Show the distribution of due dates
SELECT 
  due_date,
  TO_CHAR(due_date, 'Day') as day_name,
  COUNT(*) as creators_due
FROM (
  SELECT 
    (updated_at + INTERVAL '7 days')::date as due_date
  FROM creatordata 
  WHERE updated_at IS NOT NULL
) due_schedule
GROUP BY due_date, TO_CHAR(due_date, 'Day')
ORDER BY due_date;
