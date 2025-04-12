package client

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestLongestPrefixScorer verifies scoring based on consecutive block hits from the start.
func TestLongestPrefixScorer(t *testing.T) {
	scorer := &LongestPrefixScorer{}
	blockKeys := []string{"b1", "b2", "b3", "b4", "b5", "b6"}

	hitmap := map[string]string{
		"b1": "pod-a",
		"b2": "pod-a",
		"b3": "pod-a",
		"b4": "pod-b",
		"b5": "pod-b",
		"b6": "pod-a",
	}

	expected := map[string]float64{
		"pod-a": 3,
		"pod-b": 2,
	}

	scored, err := scorer.Score(blockKeys, hitmap)
	assert.NoError(t, err)
	for _, pod := range scored {
		assert.Equal(t, expected[pod.Name], pod.Score)
	}
}

// TestHighestBlockHitScorer verifies scoring based on the highest index where a pod has a block.
func TestHighestBlockHitScorer(t *testing.T) {
	scorer := &HighestBlockHitScorer{}
	blockKeys := []string{"b1", "b2", "b3", "b4", "b5"}

	hitmap := map[string]string{
		"b1": "pod-x",
		"b3": "pod-x",
		"b4": "pod-y",
		"b5": "pod-x",
	}

	expected := map[string]float64{
		"pod-x": 4,
		"pod-y": 3,
	}

	scored, err := scorer.Score(blockKeys, hitmap)
	assert.NoError(t, err)
	for _, pod := range scored {
		assert.Equal(t, expected[pod.Name], pod.Score)
	}
}

// TestCoverageBasedScorer verifies scoring based on total number of non-consecutive block hits.
func TestCoverageBasedScorer(t *testing.T) {
	scorer := &CoverageBasedScorer{}
	blockKeys := []string{"b1", "b2", "b3", "b4", "b5"}

	hitmap := map[string]string{
		"b1": "pod-x",
		"b2": "pod-x",
		"b3": "pod-y",
		"b4": "pod-x",
		"b5": "pod-y",
	}

	expected := map[string]float64{
		"pod-x": 3,
		"pod-y": 2,
	}

	scored, err := scorer.Score(blockKeys, hitmap)
	assert.NoError(t, err)
	for _, pod := range scored {
		assert.Equal(t, expected[pod.Name], pod.Score)
	}
}
