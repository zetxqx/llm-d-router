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

package handlers

import (
	"context"
	"testing"

	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	fwkrh "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requesthandling"
)

// testParser is a simple mock parser for testing routing logic.
type testParser struct {
	name  string
	paths []string
}

func (tp *testParser) TypedName() fwkplugin.TypedName {
	return fwkplugin.TypedName{Type: tp.name, Name: tp.name}
}
func (tp *testParser) Match() fwkrh.Match {
	return fwkrh.Match{
		Paths:     tp.paths,
		Protocols: nil,
	}
}
func (tp *testParser) ParseRequest(ctx context.Context, body []byte, headers map[string]string) (*fwkrh.ParseResult, error) {
	return nil, nil //nolint:nilnil
}
func (tp *testParser) ParseResponse(ctx context.Context, body []byte, headers map[string]string, endofStream bool) (*fwkrh.ParsedResponse, error) {
	return nil, nil //nolint:nilnil
}

func TestParserRouter(t *testing.T) {
	openai := &testParser{name: "openai", paths: []string{"v1/chat/completions", "v1/completions"}}
	anthropic := &testParser{name: "anthropic", paths: []string{"v1/messages"}}
	vertex := &testParser{name: "vertex", paths: []string{"PredictionService/ChatCompletions"}}
	router := NewParserRouter([]fwkrh.Parser{openai, anthropic, vertex})

	tests := []struct {
		name        string
		requestPath string
		wantParser  string
		expectError bool
	}{
		{
			name:        "exact match openai chat completions",
			requestPath: "/v1/chat/completions",
			wantParser:  "openai",
		},
		{
			name:        "exact match anthropic messages",
			requestPath: "/v1/messages",
			wantParser:  "anthropic",
		},
		{
			name:        "suffix match vertex chat completions",
			requestPath: "/google.cloud.aiplatform.v1beta1.PredictionService/ChatCompletions",
			wantParser:  "vertex",
		},
		{
			name:        "suffix match vertex chat completions different version",
			requestPath: "/google.cloud.aiplatform.v1.PredictionService/ChatCompletions",
			wantParser:  "vertex",
		},
		{
			name:        "path with trailing slash normalize match",
			requestPath: "/v1/messages/",
			wantParser:  "anthropic",
		},
		{
			name:        "no suffix match returns error",
			requestPath: "/unknown/path",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser, err := router.Route(tt.requestPath)
			if (err != nil) != tt.expectError {
				t.Errorf("Route(%q) error = %v, expectError %v", tt.requestPath, err, tt.expectError)
				return
			}
			if tt.expectError {
				return
			}
			if parser.TypedName().Name != tt.wantParser {
				t.Errorf("Route(%q) resolved parser = %q, want %q", tt.requestPath, parser.TypedName().Name, tt.wantParser)
			}
		})
	}
}

func TestParserRouterPriority(t *testing.T) {
	// Both plugins claim `v1/chat/completions`
	openai := &testParser{name: "openai", paths: []string{"v1/chat/completions"}}
	custom := &testParser{name: "custom", paths: []string{"v1/chat/completions"}}

	// 1. OpenAI configured first
	router1 := NewParserRouter([]fwkrh.Parser{openai, custom})
	parser1, err := router1.Route("/v1/chat/completions")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if parser1.TypedName().Name != "openai" {
		t.Errorf("expected openai to take precedence, got %s", parser1.TypedName().Name)
	}

	// 2. Custom configured first
	router2 := NewParserRouter([]fwkrh.Parser{custom, openai})
	parser2, err := router2.Route("/v1/chat/completions")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if parser2.TypedName().Name != "custom" {
		t.Errorf("expected custom to take precedence, got %s", parser2.TypedName().Name)
	}
}

func TestParserRouterWithWildcard(t *testing.T) {
	openai := &testParser{name: "openai", paths: []string{"v1/chat/completions", "v1/completions"}}
	wildcard := &testParser{name: "wildcard", paths: nil}
	router := NewParserRouter([]fwkrh.Parser{openai, wildcard})

	tests := []struct {
		name        string
		requestPath string
		wantParser  string
		expectError bool
	}{
		{
			name:        "specific match wins over wildcard",
			requestPath: "/v1/completions",
			wantParser:  "openai",
		},
		{
			name:        "wildcard match",
			requestPath: "/unknown/path",
			wantParser:  "wildcard",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser, err := router.Route(tt.requestPath)
			if (err != nil) != tt.expectError {
				t.Errorf("Route(%q) error = %v, expectError %v", tt.requestPath, err, tt.expectError)
				return
			}
			if tt.expectError {
				return
			}
			if parser.TypedName().Name != tt.wantParser {
				t.Errorf("Route(%q) resolved parser = %q, want %q", tt.requestPath, parser.TypedName().Name, tt.wantParser)
			}
		})
	}
}
