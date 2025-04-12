package tokenization

import (
	"github.com/daulet/tokenizers"
	"sync"
)

// Indexer interface defines the methods for managing tokenization data.
// It allows looking up the longest tokenization prefix for a given
// model name and prompt.
type Indexer interface {
	// AddFullTokenization adds the full tokenization of a string to the
	// indexer for a given model.
	AddFullTokenization(modelName string, text string, tokens []uint32, offsets []tokenizers.Offset)
	// FindLongestContainedTokens finds the sequence of contained tokens for
	// the longest matching prefix.
	FindLongestContainedTokens(prompt string, modelName string) []uint32
}

// ContainedTokenStore manages a collection of containedTokenTrie,
// one for each model.
type ContainedTokenStore struct {
	mu    sync.RWMutex
	tries map[string]*containedTokenTrie // Key: modelName
}

// NewContainedTokenStore creates a new indexer.
func NewContainedTokenStore() Indexer {
	return &ContainedTokenStore{
		tries: make(map[string]*containedTokenTrie),
	}
}

// AddFullTokenization adds the full tokenization of a string to the indexer for
// a given model. This is called by the async worker.
func (s *ContainedTokenStore) AddFullTokenization(modelName string, text string, tokens []uint32,
	offsets []tokenizers.Offset) {
	if text == "" || len(tokens) == 0 || len(tokens) != len(offsets) {
		return
	}

	s.mu.Lock()
	trie := s.getOrCreateTrie(modelName)
	s.mu.Unlock()

	trie.mu.Lock()
	defer trie.mu.Unlock()

	trie.addFullTokenization(text, tokens, offsets)
}

// FindLongestContainedTokens finds the sequence of contained tokens for the
// longest matching prefix.
func (s *ContainedTokenStore) FindLongestContainedTokens(prompt string, modelName string) []uint32 {
	s.mu.RLock()
	trie, ok := s.tries[modelName]
	s.mu.RUnlock()

	if !ok {
		return nil
	}

	return trie.FindLongestContainedTokens(prompt)
}

// getOrCreateTrie safely gets or creates a ContainedTokenTrie for a given
// model.
// Assumes the indexer's WRITE lock is held by the caller.
func (s *ContainedTokenStore) getOrCreateTrie(modelName string) *containedTokenTrie {
	trie, ok := s.tries[modelName]
	if !ok {
		trie = newContainedTokenTrie(modelName)
		s.tries[modelName] = trie
	}
	return trie
}

// containedTokenNode represents a node in the character-based Trie.
// TODO: consider chunking and hashing?
// It stores information about the last token fully contained within the prefix
// ending at this node.
type containedTokenNode struct {
	children map[rune]*containedTokenNode
	// lastContainedTokenID is that of the last token ending at or before this char position. 0 if none.
	lastContainedTokenID uint32
	// lastContainedTokenIndex is the index of that token in the original full list. -1 if none.
	lastContainedTokenIndex int
}

// newContainedTokenNode creates a new Trie node.
func newContainedTokenNode() *containedTokenNode {
	return &containedTokenNode{
		children:                make(map[rune]*containedTokenNode),
		lastContainedTokenIndex: -1,
		lastContainedTokenID:    0,
	}
}

// containedTokenTrie holds the root of the character-based prefix tree.
type containedTokenTrie struct {
	mu        sync.RWMutex
	root      *containedTokenNode
	modelName string
}

// newContainedTokenTrie creates an empty containedTokenTrie.
func newContainedTokenTrie(modelName string) *containedTokenTrie {
	return &containedTokenTrie{
		root:      newContainedTokenNode(),
		modelName: modelName,
	}
}

// addFullTokenization populates the Trie based on a fully tokenized string.
// It iterates through characters and determines the last contained token at
// each step.
// Assumes the caller holds the Write Lock.
func (t *containedTokenTrie) addFullTokenization(text string, tokens []uint32, offsets []tokenizers.Offset) {
	node := t.root
	lastFoundK := -1

	// Pre-check: Handle potential leading special token ([CLS]) if its offset is [0,0]
	// This ensures the root's immediate children might inherit this if applicable.
	if len(offsets) > 0 && offsets[0][0] == 0 && offsets[0][1] == 0 {
		lastFoundK = 0
	}

	for i, char := range text {
		charEndPos := uint(i + 1) // The character position *after* including the current char

		// Find the largest token index 'k' such that offsets[k][1] <= charEndPos
		// We can continue searching forward from the previously found k.
		currentBestK := lastFoundK
		searchStart := 0
		if lastFoundK != -1 {
			searchStart = lastFoundK // Start search from the last known good index
		}

		for k := searchStart; k < len(offsets); k++ {
			if offsets[k][1] <= charEndPos {
				if k > currentBestK { // Update if this index is greater
					currentBestK = k
				}
			} else {
				break
			}
		}
		lastFoundK = currentBestK // Update the overall last found k for the next iteration

		// Traverse or create the node for the current character
		child, ok := node.children[char]
		if !ok {
			child = newContainedTokenNode()
			node.children[char] = child
		}
		node = child // Move to the child node

		// Store the determined token info at this node
		if lastFoundK != -1 {
			node.lastContainedTokenIndex = lastFoundK
			node.lastContainedTokenID = tokens[lastFoundK]
		} else {
			node.lastContainedTokenIndex = -1
			node.lastContainedTokenID = 0
		}
	}
}

// FindLongestContainedTokens traverses the Trie for the prompt and collects
// the sequence of last contained tokens encountered.
func (t *containedTokenTrie) FindLongestContainedTokens(prompt string) []uint32 {
	t.mu.RLock()
	defer t.mu.RUnlock()

	containedTokens := []uint32{}
	lastTokenIdxSeen := -1 // Track the index to avoid adding duplicates from parent nodes
	node := t.root

	// Handle potential token associated with the root (e.g., leading CLS)
	if node.lastContainedTokenIndex > lastTokenIdxSeen {
		containedTokens = append(containedTokens, node.lastContainedTokenID)
		lastTokenIdxSeen = node.lastContainedTokenIndex
	}

	for _, char := range prompt {
		child, ok := node.children[char]
		if !ok {
			break
		}
		node = child

		// Check if this node represents a *new* contained token compared to the sequence so far
		if node.lastContainedTokenIndex > lastTokenIdxSeen {
			containedTokens = append(containedTokens, node.lastContainedTokenID)
			lastTokenIdxSeen = node.lastContainedTokenIndex
		}
	}

	return containedTokens
}
