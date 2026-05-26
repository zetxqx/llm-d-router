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
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	fwkrh "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requesthandling"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
)

func TestProducer_Produce_DefaultConfig(t *testing.T) {
	p, err := Factory("test", nil, plugin.NewEppHandle(context.Background(), nil))
	require.NoError(t, err)
	producer := p.(*Producer)

	testCases := []struct {
		name          string
		request       *scheduling.InferenceRequest
		expectedInput int64
		expectedOut   int64
		expectedTotal int64
	}{
		{
			name:          "Nil request",
			request:       nil,
			expectedInput: 0,
			expectedOut:   0,
			expectedTotal: 0,
		},
		{
			name:          "Empty request",
			request:       &scheduling.InferenceRequest{},
			expectedInput: 0,
			expectedOut:   0,
			expectedTotal: 0,
		},
		{
			name: "Body nil",
			request: &scheduling.InferenceRequest{
				Body: nil,
			},
			expectedInput: 0,
			expectedOut:   0,
			expectedTotal: 0,
		},
		{
			name: "Less than 4 characters",
			request: &scheduling.InferenceRequest{
				Body: &fwkrh.InferenceRequestBody{
					Completions: &fwkrh.CompletionsRequest{
						Prompt: fwkrh.Prompt{Raw: "123"},
					},
				},
			},
			expectedInput: 1, // round(3/4) = 1
			expectedOut:   2, // round(1*1.5) = 2
			expectedTotal: 3,
		},
		{
			name: "Completions Request",
			request: &scheduling.InferenceRequest{
				Body: &fwkrh.InferenceRequestBody{
					Completions: &fwkrh.CompletionsRequest{
						Prompt: fwkrh.Prompt{Raw: "Hello, world!"},
					},
				},
			},
			expectedInput: 3, // round(13/4) = 3
			expectedOut:   5, // round(3*1.5) = 5
			expectedTotal: 8,
		},
		{
			name: "Completions with empty prompt",
			request: &scheduling.InferenceRequest{
				Body: &fwkrh.InferenceRequestBody{
					Completions: &fwkrh.CompletionsRequest{
						Prompt: fwkrh.Prompt{},
					},
				},
			},
			expectedInput: 1, // math.Max(1, round(0)) = 1
			expectedOut:   2,
			expectedTotal: 3,
		},
		{
			name: "Completions with exactly 4 characters",
			request: &scheduling.InferenceRequest{
				Body: &fwkrh.InferenceRequestBody{
					Completions: &fwkrh.CompletionsRequest{
						Prompt: fwkrh.Prompt{Raw: "1234"},
					},
				},
			},
			expectedInput: 1, // round(4/4) = 1
			expectedOut:   2,
			expectedTotal: 3,
		},
		{
			name: "Chat Completions Request with Structured content",
			request: &scheduling.InferenceRequest{
				Body: &fwkrh.InferenceRequestBody{
					ChatCompletions: &fwkrh.ChatCompletionsRequest{
						Messages: []fwkrh.Message{
							{
								Role: "user",
								Content: fwkrh.Content{
									Structured: []fwkrh.ContentBlock{
										{
											Type: "text",
											Text: "This is a longer message.",
										},
									},
								},
							},
						},
					},
				},
			},
			expectedInput: 7,  // round(26/4) = 7 (includes trailing space)
			expectedOut:   11, // round(7*1.5) = 11
			expectedTotal: 18,
		},
		{
			name: "Chat Completions with Raw content",
			request: &scheduling.InferenceRequest{
				Body: &fwkrh.InferenceRequestBody{
					ChatCompletions: &fwkrh.ChatCompletionsRequest{
						Messages: []fwkrh.Message{
							{
								Role: "user",
								Content: fwkrh.Content{
									Raw: "This is raw content.",
								},
							},
						},
					},
				},
			},
			expectedInput: 6, // "user" (4) + "This is raw content." (20) = 24 / 4 = 6
			expectedOut:   9, // round(6*1.5) = 9
			expectedTotal: 15,
		},
		{
			name: "Request Size Bytes Fallback",
			request: &scheduling.InferenceRequest{
				RequestSizeBytes: 100,
				Body:             nil,
			},
			expectedInput: 25, // 100 / 4 = 25
			expectedOut:   38, // round(25*1.5) = 38
			expectedTotal: 63,
		},
		{
			name: "Input Token Count Hint Preferred",
			request: &scheduling.InferenceRequest{
				Body: &fwkrh.InferenceRequestBody{
					Completions: &fwkrh.CompletionsRequest{
						Prompt: fwkrh.Prompt{
							TokenIDs: []uint32{1, 2, 3, 4, 5}, // 5 tokens
						},
					},
				},
			},
			expectedInput: 5,
			expectedOut:   8, // round(5*1.5) = 8
			expectedTotal: 13,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := producer.Produce(context.Background(), tc.request, nil)
			require.NoError(t, err)

			if tc.request == nil {
				return
			}

			input, ok := scheduling.ReadRequestAttribute[int64](tc.request, EstimatedInputTokensKey)
			if tc.expectedInput == 0 {
				require.False(t, ok)
			} else {
				require.True(t, ok)
				require.Equal(t, tc.expectedInput, input)
			}

			output, ok := scheduling.ReadRequestAttribute[int64](tc.request, EstimatedOutputTokensKey)
			if tc.expectedOut == 0 {
				require.False(t, ok)
			} else {
				require.True(t, ok)
				require.Equal(t, tc.expectedOut, output)
			}

			total, ok := scheduling.ReadRequestAttribute[int64](tc.request, EstimatedTotalTokensKey)
			if tc.expectedTotal == 0 {
				require.False(t, ok)
			} else {
				require.True(t, ok)
				require.Equal(t, tc.expectedTotal, total)
			}
		})
	}
}

func TestProducer_Produce_CustomConfig(t *testing.T) {
	configJSON := `{"charactersPerToken": 2.0, "outputRatio": 2.0}`
	decoder := json.NewDecoder(bytes.NewBufferString(configJSON))
	p, err := Factory("test", decoder, plugin.NewEppHandle(context.Background(), nil))
	require.NoError(t, err)
	producer := p.(*Producer)

	request := &scheduling.InferenceRequest{
		Body: &fwkrh.InferenceRequestBody{
			Completions: &fwkrh.CompletionsRequest{
				Prompt: fwkrh.Prompt{Raw: "1234"},
			},
		},
	}

	err = producer.Produce(context.Background(), request, nil)
	require.NoError(t, err)

	input, ok := scheduling.ReadRequestAttribute[int64](request, EstimatedInputTokensKey)
	require.True(t, ok)
	require.Equal(t, int64(2), input) // 4 / 2 = 2

	output, ok := scheduling.ReadRequestAttribute[int64](request, EstimatedOutputTokensKey)
	require.True(t, ok)
	require.Equal(t, int64(4), output) // 2 * 2 = 4

	total, ok := scheduling.ReadRequestAttribute[int64](request, EstimatedTotalTokensKey)
	require.True(t, ok)
	require.Equal(t, int64(6), total) // 2 + 4 = 6
}

func TestProducer_Factory_Errors(t *testing.T) {
	t.Run("Invalid CharactersPerToken", func(t *testing.T) {
		configJSON := `{"charactersPerToken": 0.0}`
		decoder := json.NewDecoder(bytes.NewBufferString(configJSON))
		_, err := Factory("test", decoder, plugin.NewEppHandle(context.Background(), nil))
		require.Error(t, err)
	})

	t.Run("Invalid OutputRatio", func(t *testing.T) {
		configJSON := `{"outputRatio": -1.0}`
		decoder := json.NewDecoder(bytes.NewBufferString(configJSON))
		_, err := Factory("test", decoder, plugin.NewEppHandle(context.Background(), nil))
		require.Error(t, err)
	})
}
