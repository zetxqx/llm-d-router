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
	"encoding/binary"
	"encoding/json"
	"errors"

	"github.com/cespare/xxhash/v2"
	"sigs.k8s.io/controller-runtime/pkg/log"

	logutil "github.com/llm-d/llm-d-router/pkg/common/observability/logging"
	fwkrh "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requesthandling"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
)

func extractPromptBytes(ctx context.Context, request *scheduling.InferenceRequest, tokenEstimator TokenEstimator) ([]byte, error) {
	if request == nil || request.Body == nil {
		return nil, errors.New("request or request body is nil")
	}

	switch {
	case request.Body.Conversations != nil:
		rawBytes, err := json.Marshal(request.Body.Conversations.Items)
		if err != nil {
			return nil, errors.New("failed to marshal conversations")
		}
		return rawBytes, nil

	case request.Body.Responses != nil:
		var combined []map[string]interface{}
		if request.Body.Responses.Instructions != nil {
			combined = append(combined, map[string]interface{}{"instructions": request.Body.Responses.Instructions})
		}
		if request.Body.Responses.Tools != nil {
			combined = append(combined, map[string]interface{}{"tools": request.Body.Responses.Tools})
		}
		combined = append(combined, map[string]interface{}{"input": request.Body.Responses.Input})
		rawBytes, err := json.Marshal(combined)
		if err != nil {
			return nil, errors.New("failed to marshal responses")
		}
		return rawBytes, nil

	case request.Body.ChatCompletions != nil:
		return extractPromptBytesFromChatCompletions(ctx, request, tokenEstimator), nil

	case request.Body.Completions != nil:
		return []byte(request.Body.Completions.Prompt.PlainText()), nil

	case request.Body.Embeddings != nil:
		rawBytes, err := json.Marshal(request.Body.Embeddings.Input)
		if err != nil {
			return nil, errors.New("failed to marshal embeddings")
		}
		return rawBytes, nil

	default:
		return nil, errors.New("invalid request body: no recognized API format found")
	}
}

func extractPromptBytesFromChatCompletions(ctx context.Context, request *scheduling.InferenceRequest, tokenEstimator TokenEstimator) []byte {
	loggerDebug := log.FromContext(ctx).V(logutil.DEBUG)
	messages := request.Body.ChatCompletions.Messages
	var allPseudoBytes []byte

	for _, msg := range messages {
		if msg.Role != "" {
			allPseudoBytes = append(allPseudoBytes, []byte(msg.Role)...)
		}
		if msg.Content.Raw != "" {
			allPseudoBytes = append(allPseudoBytes, []byte(msg.Content.Raw)...)
		} else if len(msg.Content.Structured) > 0 {
			for _, block := range msg.Content.Structured {
				switch block.Type {
				case "text":
					allPseudoBytes = append(allPseudoBytes, []byte(block.Text)...)
				case "image_url":
					// multimodal content can't be in the same pseudo token of text.
					allPseudoBytes = padToAlignment(allPseudoBytes, averageCharactersPerToken)
					url := block.ImageURL.URL
					numPlaceHolders := tokenEstimator.Estimate(fwkrh.ContentBlock{
						Type:     "image_url",
						ImageURL: fwkrh.ImageBlock{URL: url},
					})

					imgHashVal := xxhash.Sum64([]byte(url))
					imgHashBytes := make([]byte, 4)
					binary.LittleEndian.PutUint32(imgHashBytes, uint32(imgHashVal))
					for i := 0; i < numPlaceHolders; i++ {
						allPseudoBytes = append(allPseudoBytes, imgHashBytes...)
					}
				case "video_url":
					// Add video support later
					allPseudoBytes = padToAlignment(allPseudoBytes, averageCharactersPerToken)
					allPseudoBytes = append(allPseudoBytes, []byte(block.VideoURL.URL)...)
				case "input_audio", "audio_url":
					// Add audio support later
					allPseudoBytes = padToAlignment(allPseudoBytes, averageCharactersPerToken)
					allPseudoBytes = append(allPseudoBytes, []byte(block.InputAudio.Data)...)
					allPseudoBytes = append(allPseudoBytes, []byte(block.InputAudio.Format)...)
				default:
					loggerDebug.Info("Unsupported block type: " + block.Type)
				}
			}
		}
	}

	return allPseudoBytes
}

func padToAlignment(b []byte, alignment int) []byte {
	remainder := len(b) % alignment
	if remainder == 0 {
		return b
	}
	padding := alignment - remainder
	for i := 0; i < padding; i++ {
		b = append(b, 0)
	}
	return b
}
