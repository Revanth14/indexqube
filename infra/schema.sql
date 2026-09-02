-- IndexQube Data Layer Schema
-- Enables pgvector for semantic response caching.

-- 1. Enable Extensions
CREATE EXTENSION IF NOT EXISTS vector;

-- 2. Response Cache (L2 Semantic Cache)
-- Stores model responses with their embedding for similarity search.
CREATE TABLE IF NOT EXISTS response_cache (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id TEXT NOT NULL,
    cache_key TEXT NOT NULL UNIQUE, -- SHA-256 of request material
    provider TEXT NOT NULL,
    model TEXT NOT NULL,
    chunks JSONB NOT NULL, -- Captured OpenAI-shaped response chunks only
    embedding VECTOR(1536), -- Sized for text-embedding-3-small
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_cache_tenant ON response_cache(tenant_id);
CREATE INDEX IF NOT EXISTS idx_cache_embedding ON response_cache USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);

-- Aggregate-only, explicitly opted-in local task reliability snapshots. This
-- table intentionally has no prompt, path, command, task ID, or output column.
CREATE TABLE IF NOT EXISTS reliability_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    machine_id TEXT NOT NULL,
    iq_version TEXT NOT NULL,
    os_arch TEXT NOT NULL,
    generated_at TIMESTAMPTZ NOT NULL,
    tasks_total BIGINT NOT NULL,
    turns_total BIGINT NOT NULL,
    turns_succeeded BIGINT NOT NULL,
    turns_failed BIGINT NOT NULL,
    turns_cancelled BIGINT NOT NULL,
    successful_latency_p50_ms BIGINT NOT NULL,
    successful_latency_p95_ms BIGINT NOT NULL,
    handoffs BIGINT NOT NULL,
    automatic_fallbacks BIGINT NOT NULL,
    verifications_passed BIGINT NOT NULL,
    verifications_warnings BIGINT NOT NULL,
    verifications_failed BIGINT NOT NULL,
    verifications_skipped BIGINT NOT NULL,
    crash_recoveries BIGINT NOT NULL,
    crash_recoveries_needing_attention BIGINT NOT NULL,
    verified_without_manual_switch BIGINT NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_reliability_events_received_at ON reliability_events(received_at);

-- 3. Utility Functions
-- Cleanup expired entries
CREATE OR REPLACE FUNCTION delete_expired_cache_entries() RETURNS void AS $$
BEGIN
    DELETE FROM response_cache WHERE expires_at < now();
END;
$$ LANGUAGE plpgsql;
