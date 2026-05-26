-- Streaming pipeline state tracking
-- The streaming pipeline uses this table to persist the last successful
-- fetch checkpoint so that restarts resume from the correct point.

CREATE SCHEMA IF NOT EXISTS streaming;

CREATE TABLE IF NOT EXISTS streaming.state (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Seed a default checkpoint if none exists (24 hours ago).
-- The streaming application will overwrite this on its first cycle.
INSERT INTO streaming.state (key, value, updated_at)
VALUES (
    'last_fetch_time',
    (NOW() - INTERVAL '24 hours')::TEXT,
    NOW()
)
ON CONFLICT (key) DO NOTHING;
