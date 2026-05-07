package openai

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

const (
	defaultEmbeddingModel = "text-embedding-3-small"
)

type embeddingRequest struct {
	Model string `json:"model"`
	Input string `json:"input"`
}

type embeddingResponse struct {
	Data []struct {
		Embedding []float32 `json:"embedding"`
	} `json:"data"`
}

// Embed generates a vector embedding for the given text using OpenAI.
func (a *Adapter) Embed(ctx context.Context, apiKey, text string) ([]float32, error) {
	reqBody, err := json.Marshal(embeddingRequest{
		Model: defaultEmbeddingModel,
		Input: text,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal embedding request: %w", err)
	}

	url := a.baseURL + "/v1/embeddings"
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(reqBody))
	if err != nil {
		return nil, fmt.Errorf("new http request: %w", err)
	}
	httpReq.Header.Set("Authorization", "Bearer "+apiKey)
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := a.client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("openai embeddings call failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
		return nil, fmt.Errorf("openai embeddings error: status=%d body=%s", resp.StatusCode, bytes.TrimSpace(body))
	}

	var ev embeddingResponse
	if err := json.NewDecoder(resp.Body).Decode(&ev); err != nil {
		return nil, fmt.Errorf("decode embedding response: %w", err)
	}

	if len(ev.Data) == 0 {
		return nil, fmt.Errorf("no embedding data returned")
	}

	return ev.Data[0].Embedding, nil
}
