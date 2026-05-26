/*
Copyright 2026 The llm-d Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
you may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package esitmatetoken

import (
	"context"
	"encoding/json"
	"fmt"
	"math"

	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requestcontrol"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
)

const (
	// PluginType is the canonical type name used to register the plugin.
	PluginType = "estimate-token-producer"

	// EstimatedInputTokensKey stores the estimated input token count (int64).
	EstimatedInputTokensKey = "estimated-input-tokens"
	// EstimatedOutputTokensKey stores the estimated output token count (int64).
	EstimatedOutputTokensKey = "estimated-output-tokens"
	// EstimatedTotalTokensKey stores the estimated total token count (int64).
	EstimatedTotalTokensKey = "estimated-total-tokens"
	// EstimatedPromptBytesKey stores the estimated raw prompt bytes ([]byte).
	EstimatedPromptBytesKey = "estimated-prompt-bytes"
)

var (
	// PromptBytesDataKey is the data key emitted by this producer.
	PromptBytesDataKey = plugin.NewDataKey("PromptBytes", PluginType)
)

// Config holds the configuration for the estimate-token producer plugin.
type Config struct {
	// CharactersPerToken is the average number of characters per token.
	// Used to estimate tokens from character count. Defaults to 4.0.
	CharactersPerToken float64 `json:"charactersPerToken"`
	// OutputRatio is the estimated ratio of output tokens to input tokens.
	// Defaults to 1.5.
	OutputRatio float64 `json:"outputRatio"`
	// MultimodalTokenEstimator configuration for the plugin.
	MultimodalTokenEstimator *MultiModalTokenEstimatorConfig `json:"multiModalTokenEstimator,omitempty"`
}

func defaultConfig() Config {
	return Config{
		CharactersPerToken:       4.0,
		OutputRatio:              1.5,
		MultimodalTokenEstimator: &DefaultMultimodalConfig,
	}
}

// Producer estimates the number of tokens for a request, extracts prompt bytes,
// and stores the results in the request attributes.
type Producer struct {
	typedName      plugin.TypedName
	config         Config
	dk             plugin.DataKey
	tokenEstimator TokenEstimator
}

var (
	_ requestcontrol.DataProducer = &Producer{}
)

// Factory is the factory function for the estimate-token producer plugin.
func Factory(name string, rawParameters *json.Decoder, handle plugin.Handle) (plugin.Plugin, error) {
	cfg := defaultConfig()
	if rawParameters != nil {
		if err := rawParameters.Decode(&cfg); err != nil {
			return nil, fmt.Errorf("failed to parse the parameters of the '%s' plugin - %w", PluginType, err)
		}
	}

	if cfg.CharactersPerToken <= 0 {
		return nil, fmt.Errorf("invalid configuration for '%s' plugin: 'charactersPerToken' must be > 0", PluginType)
	}
	if cfg.OutputRatio < 0 {
		return nil, fmt.Errorf("invalid configuration for '%s' plugin: 'outputRatio' must be >= 0", PluginType)
	}

	ctx := context.Background()
	if handle != nil && handle.Context() != nil {
		ctx = handle.Context()
	}

	return &Producer{
		typedName:      plugin.TypedName{Type: PluginType, Name: name},
		config:         cfg,
		dk:             PromptBytesDataKey.WithNonEmptyProducerName(name),
		tokenEstimator: NewTokenEstimator(ctx, cfg.MultimodalTokenEstimator),
	}, nil
}

// TypedName returns the typed name of the plugin.
func (p *Producer) TypedName() plugin.TypedName {
	return p.typedName
}

// Produces returns the data keys this plugin produces.
func (p *Producer) Produces() map[plugin.DataKey]any {
	return map[plugin.DataKey]any{
		p.dk: []byte{},
	}
}

// Produce extracts prompt bytes and estimates tokens for the request, populating attributes.
func (p *Producer) Produce(ctx context.Context, request *scheduling.InferenceRequest, _ []scheduling.Endpoint) error {
	if request == nil {
		return nil
	}

	// 1. Extract prompt bytes (pseudo-tokens)
	var promptBytes []byte
	if request.Body != nil {
		var err error
		promptBytes, err = extractPromptBytes(ctx, request, p.tokenEstimator)
		if err != nil {
			// Log error but don't fail the request, some requests might not have body (e.g. raw bytes)
			// but we can still estimate tokens if size is available.
			log := log.FromContext(ctx).WithName(p.typedName.String())
			log.V(1).Info("Failed to extract prompt bytes", "err", err)
		} else {
			request.PutAttribute(EstimatedPromptBytesKey, promptBytes)
		}
	}

	// 2. Estimate tokens
	inputTokens := p.estimateInput(request, promptBytes)
	if inputTokens <= 0 {
		return nil // Don't set attributes if we couldn't estimate
	}

	outputTokens := p.estimateOutput(inputTokens)
	totalTokens := inputTokens + outputTokens

	request.PutAttribute(EstimatedInputTokensKey, inputTokens)
	request.PutAttribute(EstimatedOutputTokensKey, outputTokens)
	request.PutAttribute(EstimatedTotalTokensKey, totalTokens)

	return nil
}

func (p *Producer) estimateInput(request *scheduling.InferenceRequest, promptBytes []byte) int64 {
	if request == nil {
		return 0
	}
	// Prefer extracted prompt bytes if body is parsed
	if request.Body != nil {
		hint := request.Body.InputTokenCountHint()
		if hint >= 0 {
			return int64(hint)
		}
		return int64(math.Max(1, math.Round(float64(len(promptBytes))/p.config.CharactersPerToken)))
	}
	// Fallback to size if body is not parsed (e.g. raw bytes)
	if request.RequestSizeBytes > 0 {
		return max(int64(request.RequestSizeBytes)/4, 1)
	}
	return 0
}

func (p *Producer) estimateOutput(inputTokens int64) int64 {
	if inputTokens <= 0 {
		return 0
	}
	return int64(math.Round(float64(inputTokens) * p.config.OutputRatio))
}
