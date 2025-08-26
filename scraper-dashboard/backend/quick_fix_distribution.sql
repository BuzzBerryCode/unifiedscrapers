-- QUICK FIX: One-liner to redistribute all creators across 7 days
-- Copy and paste this directly into Supabase SQL editor

UPDATE creatordata 
SET updated_at = (
  CURRENT_TIMESTAMP 
  - INTERVAL '1 day' * (7 - (ROW_NUMBER() OVER (ORDER BY id) - 1) % 7)
  + INTERVAL '1 hour' * (6 + ((ROW_NUMBER() OVER (ORDER BY id) - 1) * 7) % 13)
  + INTERVAL '1 minute' * (((ROW_NUMBER() OVER (ORDER BY id) - 1) * 17) % 60)
)::timestamp;

-- Verify the results:
SELECT 
  (updated_at + INTERVAL '7 days')::date as due_date,
  TO_CHAR((updated_at + INTERVAL '7 days')::date, 'Day') as day_name,
  COUNT(*) as creators_due
FROM creatordata 
WHERE updated_at IS NOT NULL
GROUP BY due_date, TO_CHAR((updated_at + INTERVAL '7 days')::date, 'Day')
ORDER BY due_date;
