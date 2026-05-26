/*
Copyright 2026 The Kubernetes Authors.

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

package approximateprefix

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/cespare/xxhash/v2"
	"github.com/stretchr/testify/assert"

	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requestcontrol/dataproducer/esitmatetoken"
)

var (
	base64Image180p1 = "data:image/jpeg;base64,/9j/4QDeRXhpZgAASUkqAAgAAAAGABIBAwABAAAAAQAAABoBBQABAAAAVgAAABsBBQABAAAAXgAAACgBAwABAAAAAgAAABMCAwABAAAAAQAAAGmHBAABAAAAZgAAAAAAAABIAAAAAQAAAEgAAAABAAAABwAAkAcABAAAADAyMTABkQcABAAAAAECAwCGkgcAFgAAAMAAAAAAoAcABAAAADAxMDABoAMAAQAAAP//AAACoAQAAQAAAEABAAADoAQAAQAAALQAAAAAAAAAQVNDSUkAAABQaWNzdW0gSUQ6IDMzOP/bAEMACAYGBwYFCAcHBwkJCAoMFA0MCwsMGRITDxQdGh8eHRocHCAkLicgIiwjHBwoNyksMDE0NDQfJzk9ODI8LjM0Mv/bAEMBCQkJDAsMGA0NGDIhHCEyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMv/CABEIALQBQAMBIgACEQEDEQH/xAAaAAADAQEBAQAAAAAAAAAAAAABAgMABAUG/8QAFwEBAQEBAAAAAAAAAAAAAAAAAAECA//aAAwSTAGE...[very long string]..."
	base64Image180p2 = "data:image/jpeg;base64,/9j/4QDeRXhpZgAASUkqAAgAAAAGABIBAwABAAAAAQAAABoBBQABAAAAVgAAABsBBQABAAAAXgAAACgBAwABAAAAAgAAABMCAwABAAAAAQAAAGmHBAABAAAAZgAAAAAAAABIAAAAAQAAAEgAAAABAAAABwAAkAcABAAAADAyMTABkQcABAAAAAECAwCGkgcAFQAAAMAAAAAAoAcABAAAADAxMDABoAMAAQAAAP//AAACoAQAAQAAAEABAAADoAQAAQAAALQAAAAAAAAAQVNDSUkAAABQaWNzdW0gSUQ6IDk5AP/bAEMACAYGBwYFCAcHBwkJCAoMFA0MCwsMGRITDxQdGh8eHRocHCAkLicgIiwjHBwoNyksMDE0NDQfJzk9ODI8LjM0Mv/bAEMBCQkJDAsMGA0NGDIhHCEyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMv/CABEIALQBQAMBIgACEQEDEQH/xAAbAAABBQEBAAAAAAAAAAAAAAAEAAIDBQYBB//EABYBAQEBAAAAAAAAAAAAAAAAAAABAv/aAAwSTAGE...[very long string]..."
)

func TestGetBlockHashes(t *testing.T) {
	tests := []struct {
		name            string
		promptBytes     []byte
		setAttr         bool
		blockSizeTokens int
		maxPrefixBlocks int
		expectNil       bool
	}{
		{
			name:            "Normal prompt bytes",
			promptBytes:     []byte("aaaabbbb"),
			setAttr:         true,
			blockSizeTokens: 1, // 4 bytes per block
			maxPrefixBlocks: 10,
			expectNil:       false,
		},
		{
			name:            "Missing attribute",
			promptBytes:     nil,
			setAttr:         false,
			blockSizeTokens: 1,
			maxPrefixBlocks: 10,
			expectNil:       true,
		},
		{
			name:            "Empty prompt bytes",
			promptBytes:     []byte(""),
			setAttr:         true,
			blockSizeTokens: 1,
			maxPrefixBlocks: 10,
			expectNil:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &fwksched.InferenceRequest{
				RequestID:   "test-req",
				TargetModel: "test-model",
			}
			if tt.setAttr {
				req.PutAttribute(esitmatetoken.EstimatedPromptBytesKey, tt.promptBytes)
			}

			hashes := getBlockHashes(context.Background(), req, tt.blockSizeTokens, tt.maxPrefixBlocks)
			if tt.expectNil {
				assert.Nil(t, hashes)
			} else {
				assert.NotNil(t, hashes)
				if tt.name == "Normal prompt bytes" {
					assert.Equal(t, 2, len(hashes)) // "aaaa", "bbbb" -> 2 blocks
				}
			}
		})
	}
}

func TestKVCacheBlock_Hash(t *testing.T) {
	tests := []struct {
		name     string
		blockA   HashBlock
		blockB   HashBlock
		shouldEq bool
	}{
		{
			name: "Identical Blocks",
			blockA: HashBlock{
				PseudoTokens: []byte("Hello"),
				Tokens:       []uint32{1, 2},
			},
			blockB: HashBlock{
				PseudoTokens: []byte("Hello"),
				Tokens:       []uint32{1, 2},
			},
			shouldEq: true,
		},
		{
			name: "Different PseudoBytes Content",
			blockA: HashBlock{
				PseudoTokens: []byte("Hello"),
			},
			blockB: HashBlock{
				PseudoTokens: []byte("Hellp"),
			},
			shouldEq: false,
		},
		{
			name: "Different Token IDs",
			blockA: HashBlock{
				Tokens: []uint32{1, 2},
			},
			blockB: HashBlock{
				Tokens: []uint32{1, 3},
			},
			shouldEq: false,
		},
		{
			name:     "Empty fields match",
			blockA:   HashBlock{},
			blockB:   HashBlock{},
			shouldEq: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hashA := tt.blockA.Hash()
			hashB := tt.blockB.Hash()
			if tt.shouldEq {
				assert.Equal(t, hashA, hashB)
			} else {
				assert.NotEqual(t, hashA, hashB)
			}
		})
	}
}

func repeatBytes(b []byte, count int) []byte {
	res := make([]byte, 0, len(b)*count)
	for i := 0; i < count; i++ {
		res = append(res, b...)
	}
	return res
}

func imageHashBytes(url string) []byte {
	h := xxhash.Sum64([]byte(url))
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, uint32(h))
	return buf
}
