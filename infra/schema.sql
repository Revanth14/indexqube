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

-- 3. Utility Functions
-- Cleanup expired entries
CREATE OR REPLACE FUNCTION delete_expired_cache_entries() RETURNS void AS $$
BEGIN
    DELETE FROM response_cache WHERE expires_at < now();
END;
$$ LANGUAGE plpgsql;
