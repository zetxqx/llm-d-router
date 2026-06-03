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

// Config holds the configuration for the Parser.
type Config struct {
	Parsers []fwkrh.Parser
}

func (c *Config) String() string {
	if c == nil {
		return "<nil>"
	}
	// Define a local type definition to prevent infinite recursion when calling Sprintf("%+v").
	// A new type definition inherits the struct fields but does not copy its methods,
	// bypassing the Stringer check and allowing a safe reflection-based field dump.
	type temp Config
	return fmt.Sprintf("%+v", temp(*c))
}

// NewParsers returns the configured parsers.
func NewParsers(config *Config) []fwkrh.Parser {
	return config.Parsers
}

// ParserRouter handles the routing of incoming requests to the appropriate Parser.
type ParserRouter struct {
	routes         map[string]fwkrh.Parser
	fallbackParser fwkrh.Parser
}

// NewParserRouter builds a central routing table from a list of active parsers.
// The order of the input parsers determines the priority (first match wins for identical suffixes).
func NewParserRouter(parsers []fwkrh.Parser) *ParserRouter {
	router := &ParserRouter{
		routes: make(map[string]fwkrh.Parser),
	}

	for _, parser := range parsers {
		paths := parser.Match().Paths
		if len(paths) == 0 {
			// A parser with no paths acts as a fallback. Stop processing subsequent
			// parsers once a fallback is registered, as it will capture all remaining traffic.
			if router.fallbackParser == nil {
				router.fallbackParser = parser
			}
			break
		}
		for _, suffix := range paths {
			normalized := normalizeSuffix(suffix)
			// First-wins priority for duplicate suffixes
			if _, exists := router.routes[normalized]; !exists {
				router.routes[normalized] = parser
			}
		}
	}

	return router
}

// Route resolves an incoming request path to the matching Parser using suffix matching.
func (pr *ParserRouter) Route(path string) (fwkrh.Parser, error) {
	for suffix, parser := range pr.routes {
		if request.MatchPathSuffix(path, suffix) {
			return parser, nil
		}
	}
	if pr.fallbackParser != nil {
		return pr.fallbackParser, nil
	}

	return nil, fmt.Errorf("no parser registered matching path suffix for: %s", path)
}

// normalizeSuffix cleans up a suffix by removing leading and trailing slashes.
func normalizeSuffix(suffix string) string {
	return strings.Trim(strings.TrimSpace(suffix), "/")
}
