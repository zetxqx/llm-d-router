/*
Copyright 2026 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package inflightload

import (
	"testing"

	"github.com/stretchr/testify/require"

	fwkrh "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requesthandling"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
)

func TestSimpleTokenEstimator_Estimate(t *testing.T) {
	estimator := NewSimpleTokenEstimator()

	testCases := []struct {
		name     string
		request  *fwksched.InferenceRequest
		expected int64
	}{
		{
			name:     "Nil request",
			request:  nil,
			expected: 0,
		},
		{
			name:     "Empty request",
			request:  &fwksched.InferenceRequest{},
			expected: 0,
		},
		{
			name: "Body nil",
			request: &fwksched.InferenceRequest{
				Body: nil,
			},
			expected: 0,
		},
		{
			name: "Less than 4 characters",
			request: &fwksched.InferenceRequest{
				Body: &fwkrh.InferenceRequestBody{
					Completions: &fwkrh.CompletionsRequest{
						Prompt: fwkrh.Prompt{Raw: "123"},
					},
				},
				RequestSizeBytes: 3,
			},
			expected: 3, // max(3/4, 1) = 1. 1 + round(1*1.5) = 3
		},
		{
			name: "Completions Request",
			request: &fwksched.InferenceRequest{
				Body: &fwkrh.InferenceRequestBody{
					Completions: &fwkrh.CompletionsRequest{
						Prompt: fwkrh.Prompt{Raw: "Hello, world!"},
					},
				},
				RequestSizeBytes: 13,
			},
			expected: 8, // max(13/4, 1) = 3. 3 + round(3*1.5) = 8
		},
		{
			name: "Completions with empty prompt",
			request: &fwksched.InferenceRequest{
				Body: &fwkrh.InferenceRequestBody{
					Completions: &fwkrh.CompletionsRequest{
						Prompt: fwkrh.Prompt{},
					},
				},
				RequestSizeBytes: 0,
			},
			expected: 3, // max(0/4, 1) = 1. 1 + round(1*1.5) = 3
		},
		{
			name: "Completions with exactly 4 characters",
			request: &fwksched.InferenceRequest{
				Body: &fwkrh.InferenceRequestBody{
					Completions: &fwkrh.CompletionsRequest{
						Prompt: fwkrh.Prompt{Raw: "1234"},
					},
				},
				RequestSizeBytes: 4,
			},
			expected: 3, // max(4/4, 1) = 1. 1 + round(1*1.5) = 3
		},
		{
			name: "Chat Completions Request with Structured content",
			request: &fwksched.InferenceRequest{
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
				RequestSizeBytes: 26,
			},
			expected: 15, // max(26/4, 1) = 6. 6 + round(6*1.5) = 15
		},
		{
			name: "Chat Completions with Raw content",
			request: &fwksched.InferenceRequest{
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
				RequestSizeBytes: 21,
			},
			expected: 13, // max(21/4, 1) = 5. 5 + round(5*1.5) = 13
		},
		{
			name: "Chat Completions with multiple messages",
			request: &fwksched.InferenceRequest{
				Body: &fwkrh.InferenceRequestBody{
					ChatCompletions: &fwkrh.ChatCompletionsRequest{
						Messages: []fwkrh.Message{
							{
								Role: "user",
								Content: fwkrh.Content{
									Structured: []fwkrh.ContentBlock{
										{Type: "text", Text: "Hi"},
									},
								},
							},
							{
								Role: "assistant",
								Content: fwkrh.Content{
									Structured: []fwkrh.ContentBlock{
										{Type: "text", Text: "Hello"},
									},
								},
							},
						},
					},
				},
				RequestSizeBytes: 11,
			},
			expected: 5, // max(11/4, 1) = 2. 2 + round(2*1.5) = 5
		},
		{
			name: "Chat Completions with empty messages",
			request: &fwksched.InferenceRequest{
				Body: &fwkrh.InferenceRequestBody{
					ChatCompletions: &fwkrh.ChatCompletionsRequest{
						Messages: []fwkrh.Message{},
					},
				},
				RequestSizeBytes: 0,
			},
			expected: 3, // max(0/4, 1) = 1. 1 + round(1*1.5) = 3
		},
		{
			name: "Responses API with string input",
			request: &fwksched.InferenceRequest{
				Body: &fwkrh.InferenceRequestBody{
					Responses: &fwkrh.ResponsesRequest{
						Input: "Tell me a story about a brave knight.",
					},
				},
				RequestSizeBytes: 37,
			},
			expected: 23, // max(37/4, 1) = 9. 9 + round(9*1.5) = 23
		},
		{
			name: "Responses API with structured input",
			request: &fwksched.InferenceRequest{
				Body: &fwkrh.InferenceRequestBody{
					Responses: &fwkrh.ResponsesRequest{
						Input: []any{
							map[string]any{"role": "user", "content": "Hello"},
						},
					},
				},
				RequestSizeBytes: 34,
			},
			expected: 20, // max(34/4, 1) = 8. 8 + round(8*1.5) = 20
		},
		{
			name: "Conversations API",
			request: &fwksched.InferenceRequest{
				Body: &fwkrh.InferenceRequestBody{
					Conversations: &fwkrh.ConversationsRequest{
						Items: []fwkrh.ConversationItem{
							{Type: "message", Role: "user", Content: "Hi there"},
						},
					},
				},
				RequestSizeBytes: 55,
			},
			expected: 33, // max(55/4, 1) = 13. 13 + round(13*1.5) = 33
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual := estimator.Estimate(tc.request)
			require.Equal(t, tc.expected, actual)
		})
	}
}

func TestSimpleTokenEstimator_Estimate_CustomOutputRatio(t *testing.T) {
	estimator := &SimpleTokenEstimator{
		OutputRatio: 2.0,
	}

	testCases := []struct {
		name     string
		request  *fwksched.InferenceRequest
		expected int64
	}{
		{
			name: "Request with 4 estimated input tokens",
			request: &fwksched.InferenceRequest{
				Body: &fwkrh.InferenceRequestBody{
					Completions: &fwkrh.CompletionsRequest{
						Prompt: fwkrh.Prompt{Raw: "1234567890123456"},
					},
				},
				RequestSizeBytes: 16,
			},
			expected: 12, // 4 input. Output: 4 * 2.0 = 8. Total: 4 + 8 = 12
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual := estimator.Estimate(tc.request)
			require.Equal(t, tc.expected, actual)
		})
	}
}
