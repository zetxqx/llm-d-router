/*
Copyright 2026 The llm-d Authors.

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

package record

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIDsRoundTrip(t *testing.T) {
	ids := []uint64{0, 1, 1 << 63, 0xdeadbeefcafebabe}
	got, err := DecodeIDs(EncodeIDs(ids))
	require.NoError(t, err)
	assert.Equal(t, ids, got)

	_, err = DecodeIDs("AAA=")
	assert.Error(t, err)
}

func TestValidate(t *testing.T) {
	three := EncodeIDs([]uint64{1, 2, 3})
	tests := []struct {
		name    string
		rec     Record
		wantErr bool
	}{
		{name: "keyframe", rec: Record{V: Version, Kf: true, Blocks: 3, New: three}},
		{name: "delta", rec: Record{V: Version, Blocks: 5, Anchor: &Anchor{N: 2, H: HexID(9)}, New: three}},
		{name: "empty prompt", rec: Record{V: Version, Kf: true, New: EncodeIDs(nil)}},
		{name: "wrong version", rec: Record{V: 2, Kf: true, Blocks: 3, New: three}, wantErr: true},
		{name: "count mismatch", rec: Record{V: Version, Kf: true, Blocks: 4, New: three}, wantErr: true},
		{name: "keyframe with anchor", rec: Record{V: Version, Kf: true, Blocks: 5, Anchor: &Anchor{N: 2}, New: three}, wantErr: true},
		{name: "delta without anchor", rec: Record{V: Version, Blocks: 3, New: three}, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.rec.Validate()
			if tc.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
