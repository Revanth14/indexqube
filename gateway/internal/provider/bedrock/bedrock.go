// Package bedrock is a governor.Adapter implementation for the AWS Bedrock
// ConverseStream API.
//
// It translates the canonical InferenceRequest into the Bedrock Converse
// shape and streams tokens back through the TokenWriter.
package bedrock

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/Revanth14/indexqube/gateway/internal/domain"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"
)

// Adapter implements governor.Adapter via AWS Bedrock ConverseStream.
type Adapter struct {
	client *bedrockruntime.Client
	region string
	logger *slog.Logger
}

// Option configures an Adapter at construction time.
type Option func(*Adapter)

// WithRegion sets the AWS region.
func WithRegion(region string) Option {
	return func(a *Adapter) {
		if region != "" {
			a.region = region
		}
	}
}

// WithLogger overrides the default slog.Default() logger.
func WithLogger(l *slog.Logger) Option {
	return func(a *Adapter) {
		if l != nil {
			a.logger = l
		}
	}
}

// New returns a wired Adapter. It attempts to load the default AWS config
// if no client is provided.
func New(opts ...Option) *Adapter {
	a := &Adapter{
		region: "us-east-1",
		logger: slog.Default(),
	}
	for _, opt := range opts {
		opt(a)
	}
	return a
}

// Ready reports whether the adapter is ready.
func (a *Adapter) Ready(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// Note: lazy initialization of client might happen in Dispatch if needed,
	// but for MAANG quality we should be ready if possible.
	return nil
}

// Dispatch is the governor.Adapter implementation.
func (a *Adapter) Dispatch(ctx context.Context, req *domain.InferenceRequest, tw domain.TokenWriter) error {
	client, err := a.getClient(ctx, req)
	if err != nil {
		return fmt.Errorf("bedrock client: %w", err)
	}

	input := &bedrockruntime.ConverseStreamInput{
		ModelId:  aws.String(req.Model),
		Messages: translateMessages(req.Messages),
		InferenceConfig: &types.InferenceConfiguration{
			MaxTokens:   aws.Int32(int32(req.MaxTokens)),
			Temperature: aws.Float32(float32(req.Temperature)),
		},
	}
	if input.InferenceConfig.MaxTokens == nil || *input.InferenceConfig.MaxTokens == 0 {
		input.InferenceConfig.MaxTokens = aws.Int32(4096)
	}

	// Pull system messages into the top-level System field.
	system := translateSystem(req.Messages)
	if len(system) > 0 {
		input.System = system
	}

	output, err := client.ConverseStream(ctx, input)
	if err != nil {
		return fmt.Errorf("bedrock converse stream: %w", err)
	}

	chunkID := newChunkID()
	created := time.Now().Unix()
	
	stream := output.GetStream()
	defer stream.Close()

	for event := range stream.Events() {
		if err := ctx.Err(); err != nil {
			return err
		}

		switch v := event.(type) {
		case *types.ConverseStreamOutputMemberContentBlockDelta:
			if delta, ok := v.Value.Delta.(*types.ContentBlockDeltaMemberText); ok {
				if err := emitChunk(tw, chunkID, req.Model, created, delta.Value, ""); err != nil {
					return err
				}
			}
		case *types.ConverseStreamOutputMemberMessageStop:
			reason := mapStopReason(v.Value.StopReason)
			if err := emitChunk(tw, chunkID, req.Model, created, "", reason); err != nil {
				return err
			}
		case *types.ConverseStreamOutputMemberMessageStart:
			// Emit role frame
			if err := emitChunk(tw, chunkID, req.Model, created, "", ""); err != nil {
				return err
			}
		case *types.ConverseStreamOutputMemberMetadata:
			// Usage stats etc; can be logged
		}
	}

	if err := stream.Err(); err != nil {
		return fmt.Errorf("bedrock stream error: %w", err)
	}

	return nil
}

func (a *Adapter) getClient(ctx context.Context, req *domain.InferenceRequest) (*bedrockruntime.Client, error) {
	if a.client != nil {
		return a.client, nil
	}

	region := a.region
	if req.AWSRegion != "" {
		region = req.AWSRegion
	}

	cred := req.Credential.APIKey
	
	cfgOpts := []func(*config.LoadOptions) error{
		config.WithRegion(region),
	}

	if cred != "" {
		parts := strings.Split(cred, ":")
		if len(parts) >= 2 {
			ak, sk := parts[0], parts[1]
			st := ""
			if len(parts) >= 3 {
				st = parts[2]
			}
			cfgOpts = append(cfgOpts, config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(ak, sk, st)))
		}
	}

	cfg, err := config.LoadDefaultConfig(ctx, cfgOpts...)
	if err != nil {
		return nil, err
	}
	return bedrockruntime.NewFromConfig(cfg), nil
}

func translateMessages(msgs []domain.Message) []types.Message {
	var out []types.Message
	for _, m := range msgs {
		if m.Role == "system" {
			continue
		}
		role := types.ConversationRoleUser
		if m.Role == "assistant" {
			role = types.ConversationRoleAssistant
		}
		out = append(out, types.Message{
			Role: role,
			Content: []types.ContentBlock{
				&types.ContentBlockMemberText{Value: m.Content},
			},
		})
	}
	return out
}

func translateSystem(msgs []domain.Message) []types.SystemContentBlock {
	var out []types.SystemContentBlock
	for _, m := range msgs {
		if m.Role == "system" {
			out = append(out, &types.SystemContentBlockMemberText{Value: m.Content})
		}
	}
	return out
}

func emitChunk(tw domain.TokenWriter, id, model string, created int64, content, finishReason string) error {
	choice := openAIChoice{Index: 0}
	if content != "" {
		choice.Delta.Content = content
	} else if finishReason == "" {
		choice.Delta.Role = "assistant"
	}
	if finishReason != "" {
		fr := finishReason
		choice.FinishReason = &fr
	}

	chunk := openAIChunk{
		ID:      id,
		Object:  "chat.completion.chunk",
		Created: created,
		Model:   model,
		Choices: []openAIChoice{choice},
	}
	b, err := json.Marshal(chunk)
	if err != nil {
		return err
	}
	return tw.WriteData(b)
}

func mapStopReason(r types.StopReason) string {
	switch r {
	case types.StopReasonEndTurn:
		return "stop"
	case types.StopReasonMaxTokens:
		return "length"
	case types.StopReasonStopSequence:
		return "stop"
	case types.StopReasonToolUse:
		return "tool_calls"
	default:
		return "stop"
	}
}

func newChunkID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return "chatcmpl-" + hex.EncodeToString(b[:])
}

// --- OpenAI-compatible shapes ---

type openAIChunk struct {
	ID      string         `json:"id"`
	Object  string         `json:"object"`
	Created int64          `json:"created"`
	Model   string         `json:"model"`
	Choices []openAIChoice `json:"choices"`
}

type openAIChoice struct {
	Index        int         `json:"index"`
	Delta        openAIDelta `json:"delta"`
	FinishReason *string     `json:"finish_reason"`
}

type openAIDelta struct {
	Role    string `json:"role,omitempty"`
	Content string `json:"content,omitempty"`
}
