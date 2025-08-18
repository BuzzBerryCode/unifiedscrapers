-- Add updated_at column to creatordata table
-- This column will track when records are last modified

ALTER TABLE public.creatordata 
ADD COLUMN updated_at timestamp without time zone;

-- Add a comment to document the column
COMMENT ON COLUMN public.creatordata.updated_at IS 'Timestamp when the record was last updated/rescraped';

-- Create an index on updated_at for better query performance
CREATE INDEX idx_creatordata_updated_at ON public.creatordata(updated_at);
