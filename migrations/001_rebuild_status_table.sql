-- ============================================================================
-- REBUILD STATUS TABLE
-- ============================================================================
-- Track progress of Railway rebuild worker jobs
-- Run this migration once in Supabase SQL Editor
-- ============================================================================

CREATE TABLE IF NOT EXISTS rebuild_status (
    id SERIAL PRIMARY KEY,
    run_id TEXT UNIQUE NOT NULL,
    status TEXT NOT NULL DEFAULT 'starting',  -- starting, extracting, transforming, loading, refreshing, completed, failed
    phase TEXT,
    phase_number INTEGER DEFAULT 0,
    total_phases INTEGER DEFAULT 7,
    message TEXT,
    rows_processed INTEGER DEFAULT 0,
    error_message TEXT,
    duration_seconds NUMERIC(10,2),
    triggered_by TEXT DEFAULT 'manual',  -- manual, cron, api
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    completed_at TIMESTAMPTZ
);

-- Index for querying recent runs
CREATE INDEX IF NOT EXISTS idx_rebuild_status_created_at ON rebuild_status(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_rebuild_status_status ON rebuild_status(status);

-- RLS Policy (allow service role full access)
ALTER TABLE rebuild_status ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Service role can manage rebuild_status" ON rebuild_status
    FOR ALL
    TO service_role
    USING (true)
    WITH CHECK (true);

-- Optional: Allow authenticated users to read status
CREATE POLICY "Authenticated users can view rebuild_status" ON rebuild_status
    FOR SELECT
    TO authenticated
    USING (true);

COMMENT ON TABLE rebuild_status IS 'Tracks progress of Railway rebuild worker jobs';
