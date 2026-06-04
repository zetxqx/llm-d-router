/*
Copyright 2025 The Kubernetes Authors.

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
	"fmt"
	"strings"

	"github.com/llm-d/llm-d-router/pkg/epp/framework/common/request"
	fwkrh "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requesthandling"
)

// ParserDispatcher handles the routing of incoming requests to the appropriate Parser.
type ParserDispatcher struct {
	routes         map[string]fwkrh.Parser
	fallbackParser fwkrh.Parser
	parsers        []fwkrh.Parser
}

// NewParserDispatcher builds a central routing table from a list of active parsers.
// The order of the input parsers determines the priority (first match wins for identical suffixes).
func NewParserDispatcher(parsers []fwkrh.Parser) *ParserDispatcher {
	dispatcher := &ParserDispatcher{
		routes: make(map[string]fwkrh.Parser),
	}
	seen := make(map[fwkrh.Parser]bool)
	for _, parser := range parsers {
		if !seen[parser] {
			seen[parser] = true
			dispatcher.parsers = append(dispatcher.parsers, parser)
		}

		paths := parser.Claims().Paths
		if len(paths) == 0 {
			// A parser with no paths acts as a fallback. Stop processing subsequent
			// parsers once a fallback is registered, as it will capture all remaining traffic.
			if dispatcher.fallbackParser == nil {
				dispatcher.fallbackParser = parser
			}
			break
		}
		for _, suffix := range paths {
			normalized := normalizeSuffix(suffix)
			// First-wins priority for duplicate suffixes
			if _, exists := dispatcher.routes[normalized]; !exists {
				dispatcher.routes[normalized] = parser
			}
		}
	}

	return dispatcher
}

// Parsers returns all unique active parsers registered in the dispatcher.
func (pd *ParserDispatcher) Parsers() []fwkrh.Parser {
	return pd.parsers
}

// Dispatch resolves an incoming request path to the matching Parser using suffix matching.
func (pd *ParserDispatcher) Dispatch(path string) (fwkrh.Parser, error) {
	for suffix, parser := range pd.routes {
		if request.MatchPathSuffix(path, suffix) {
			return parser, nil
		}
	}
	if pd.fallbackParser != nil {
		return pd.fallbackParser, nil
	}

	return nil, fmt.Errorf("no parser registered matching path suffix for: %s", path)
}

// normalizeSuffix cleans up a suffix by removing leading and trailing slashes.
func normalizeSuffix(suffix string) string {
	return strings.Trim(strings.TrimSpace(suffix), "/")
}
