package supabase

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Revanth14/indexqube/gateway/internal/cache"
	"github.com/Revanth14/indexqube/gateway/internal/domain"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Cache implements cache.Cache using a Supabase (PostgreSQL) backend.
type Cache struct {
	pool          *pgxpool.Pool
	maxEntryBytes int64
}

// NewCache returns a wired Cache.
func NewCache(pool *pgxpool.Pool, maxEntryBytes int64) *Cache {
	return &Cache{
		pool:          pool,
		maxEntryBytes: maxEntryBytes,
	}
}

func (c *Cache) Get(ctx context.Context, key cache.Key) (*cache.Entry, bool, error) {
	query := `
		SELECT provider, model, chunks, created_at
		FROM response_cache
		WHERE cache_key = $1 AND (expires_at IS NULL OR expires_at > now())
		LIMIT 1
	`
	var providerStr, model string
	var chunksJSON []byte
	var createdAt time.Time

	err := c.pool.QueryRow(ctx, query, string(key)).Scan(&providerStr, &model, &chunksJSON, &createdAt)
	if err != nil {
		return nil, false, nil
	}

	var chunks [][]byte
	if err := json.Unmarshal(chunksJSON, &chunks); err != nil {
		return nil, false, fmt.Errorf("unmarshal chunks: %w", err)
	}

	return &cache.Entry{
		Provider:  domain.Provider(providerStr),
		Model:     model,
		Chunks:    chunks,
		CreatedAt: createdAt,
	}, true, nil
}

func (c *Cache) GetSemantic(ctx context.Context, tenantID string, embedding []float32, threshold float64) (*cache.Entry, bool, error) {
	if tenantID == "" || len(embedding) == 0 {
		return nil, false, nil
	}
	// Vector similarity search using cosine distance (1 - similarity)
	// We want similarity > threshold, so distance < (1 - threshold)
	query := `
		SELECT provider, model, chunks, created_at
		FROM response_cache
		WHERE tenant_id = $1 
		  AND (expires_at IS NULL OR expires_at > now())
		  AND embedding <=> $2 < $3
		ORDER BY embedding <=> $2 ASC
		LIMIT 1
	`
	var providerStr, model string
	var chunksJSON []byte
	var createdAt time.Time

	err := c.pool.QueryRow(ctx, query, tenantID, embedding, 1.0-threshold).Scan(&providerStr, &model, &chunksJSON, &createdAt)
	if err != nil {
		return nil, false, nil
	}

	var chunks [][]byte
	if err := json.Unmarshal(chunksJSON, &chunks); err != nil {
		return nil, false, fmt.Errorf("unmarshal chunks: %w", err)
	}

	return &cache.Entry{
		Provider:  domain.Provider(providerStr),
		Model:     model,
		Chunks:    chunks,
		CreatedAt: createdAt,
	}, true, nil
}

func (c *Cache) Put(ctx context.Context, key cache.Key, entry *cache.Entry) error {
	return c.put(ctx, "exact", key, entry, nil)
}

func (c *Cache) PutSemantic(ctx context.Context, tenantID string, key cache.Key, entry *cache.Entry, embedding []float32) error {
	if tenantID == "" {
		tenantID = "unknown"
	}
	return c.put(ctx, tenantID, key, entry, embedding)
}

func (c *Cache) put(ctx context.Context, tenantID string, key cache.Key, entry *cache.Entry, embedding []float32) error {
	if entry == nil {
		return nil
	}
	if c.maxEntryBytes > 0 && entry.Bytes() > c.maxEntryBytes {
		return cache.ErrEntryTooLarge
	}

	chunksJSON, err := json.Marshal(entry.Chunks)
	if err != nil {
		return fmt.Errorf("marshal chunks: %w", err)
	}

	query := `
		INSERT INTO response_cache (tenant_id, cache_key, provider, model, chunks, embedding, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (cache_key) DO UPDATE SET
			chunks = EXCLUDED.chunks,
			embedding = COALESCE(EXCLUDED.embedding, response_cache.embedding),
			created_at = EXCLUDED.created_at
	`
	_, err = c.pool.Exec(ctx, query, tenantID, string(key), string(entry.Provider), entry.Model, chunksJSON, embedding, entry.CreatedAt)
	return err
}
