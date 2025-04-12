// File: prefix_hash_table.go
package prefixhashtable

import (
	"math/rand"
	"sync"
	"time"

	"github.com/cespare/xxhash/v2"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/sirupsen/logrus"
)

const (
	// DefaultBlockSize defines how many tokens each block contains in the prefix cache.
	DefaultBlockSize = 4
	// DefaultMaxBlockNumber sets the maximum number of blocks the LRU cache can store.
	DefaultMaxBlockNumber = 500000
)

// Config contains initialization settings for PrefixHashTable (block size and cache size).
type Config struct {
	BlockNumber int
	BlockSize   int
}

// PrefixHashTable is an in-memory prefix-to-block cache with xxhash keys and LRU eviction.
// TODO: see if can use
type PrefixHashTable struct {
	mu        sync.RWMutex
	seed      uint64
	cacheSize int
	blockSize int
	store     *lru.Cache[uint64, Block]
	logger    *logrus.Entry
}

// Block holds a token slice and a list of pods that accessed it.
type Block struct {
	Tokens []uint32
	Pods   []PodAccess
}

// PodAccess represents a pod and the last time it accessed a block.
type PodAccess struct {
	Name     string
	Accessed time.Time
}

// GetPrefixBlocks retrieves all blocks for a given prompt by hashing and matching.
// GetPrefixBlocks retrieves all blocks matching the prefix hashes from the prompt.
//
//nolint:gocritic // no need named return values here
func (c *PrefixHashTable) GetPrefixBlocks(prompt []string) (map[uint64]Block, []uint64) {
	prefixHashes := c.getPrefixHashes(prompt)
	matchedBlocks := c.getPrefixBlocks(prefixHashes)
	return matchedBlocks, prefixHashes
}

// AddTokenPrefix add tokens for specific hash.
func (c *PrefixHashTable) AddTokenPrefix(prefixHash uint64, tokens []uint32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.store.Get(prefixHash)
	if !ok {
		block := Block{
			Tokens: tokens,
			Pods:   []PodAccess{{Name: "", Accessed: time.Now()}},
		}
		c.store.Add(prefixHash, block)
		c.logger.Debugf("prefixHash %v inserted block: %+v", prefixHash, block)
	}
}

// UpdatePodPrefix updates the pod access info for a specific hash.
func (c *PrefixHashTable) UpdatePodPrefix(prefixHash uint64, pod string) {
	block, ok := c.store.Get(prefixHash)
	if !ok {
		block = Block{
			Pods: []PodAccess{{Name: pod, Accessed: time.Now()}},
		}
	} else {
		block.Pods = append(block.Pods, PodAccess{
			Name:     pod,
			Accessed: time.Now(),
		})
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	c.store.Add(prefixHash, block)
}

// GetPrefixHashes returns the xxhash hashes for all blocks in the prompt.
func (c *PrefixHashTable) GetPrefixHashes(prompt []string) []uint64 {
	return c.getPrefixHashes(prompt)
}

// getPrefixBlocks fetches blocks from the cache by hash and stops on first cache miss.
func (c *PrefixHashTable) getPrefixBlocks(prefixHashes []uint64) map[uint64]Block {
	c.mu.RLock()
	defer c.mu.RUnlock()

	matchBlocks := make(map[uint64]Block)
	for _, prefixHash := range prefixHashes {
		block, ok := c.store.Get(prefixHash)
		if !ok { // early-stop
			break
		}
		matchBlocks[prefixHash] = block
	}
	return matchBlocks
}

// getPrefixHashes is the internal implementation for computing block hashes from prompt.
func (c *PrefixHashTable) getPrefixHashes(prompt []string) []uint64 {
	digest := xxhash.New()
	var hashes []uint64

	for i := 0; i < len(prompt); i += c.blockSize {
		end := i + c.blockSize
		if end > len(prompt) {
			end = len(prompt)
		}
		digest.Reset()
		for j := i; j < end; j++ {
			pBytes := []byte(prompt[j])
			_, err := digest.Write(pBytes)
			if err != nil {
				c.logger.Errorf("digest.Write in getPrefixHashes failed: %v", err)
			}
		}
		hashes = append(hashes, digest.Sum64())
	}
	return hashes
}

// ChunkPrompt splits the prompt into blocks of blockSize without hashing.
// Each block is a slice of words from the original prompt.
func (c *PrefixHashTable) ChunkPrompt(prompt []string) [][]string {
	var blocks [][]string

	for i := 0; i < len(prompt); i += c.blockSize {
		end := i + c.blockSize
		if end > len(prompt) {
			end = len(prompt)
		}
		block := prompt[i:end]
		blocks = append(blocks, block)
	}

	return blocks
}

// NewPrefixHashTable initializes the PrefixHashTable with LRU cache and random hash seed.
func NewPrefixHashTable(cfg *Config) *PrefixHashTable {
	logger := logrus.WithField("component", "Indexer.prefixCache")
	logger.Info("Start Indexer.prefixCache from type PrefixHashTable")
	blockNumber := DefaultMaxBlockNumber
	blockSize := DefaultBlockSize

	if cfg != nil {
		if cfg.BlockNumber > 0 {
			blockNumber = cfg.BlockNumber
		}
		if cfg.BlockSize > 0 {
			blockSize = cfg.BlockSize
		}
	}

	seed := rand.New(rand.NewSource(time.Now().Unix())).Uint64() // #nosec G404

	store, err := lru.New[uint64, Block](blockNumber)
	if err != nil {
		logger.Fatalf("create new LRU failed: %v", err)
	}

	return &PrefixHashTable{
		seed:      seed,
		cacheSize: blockNumber,
		blockSize: blockSize,
		store:     store,
		logger:    logger,
	}
}
