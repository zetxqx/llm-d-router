package kvindex

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"github.com/neuralmagic/distributed-kv-cache/pkg/utils"
)

// TokenProcessor defines the interface for converting tokens to
// KVBlockKeys.
type TokenProcessor interface {
	// TokensToKVBlockKeys converts tokens into KVBlockKeys.
	TokensToKVBlockKeys(tokens []uint32, modelName string) []KVBlockKey
}

// KVBlockKey is equivalent to the LMCacheEngineKey in the Python code.
type KVBlockKey struct {
	Fmt       string
	ModelName string
	WorldSize int
	WorkerID  int
	ChunkHash string
}

// String returns a string representation of the CacheEngineKey.
func (c KVBlockKey) String() string {
	/*
	   def to_string(self):
	       return f"{self.fmt}@{self.model_name}@{self.world_size}"\
	           f"@{self.worker_id}@{self.chunk_hash}"
	*/

	return fmt.Sprintf("%s@%s@%d@%d@%s", c.Fmt, c.ModelName, c.WorldSize, c.WorkerID, c.ChunkHash)
}

// LMCacheEngineConfig holds the configuration for the token database.
type LMCacheEngineConfig struct {
	ChunkSize int
}

// LMCacheEngineMetadata holds metadata used to populate the cache key.
type LMCacheEngineMetadata struct {
	Fmt       string
	WorldSize int
	WorkerID  int
}

// ChunkedTokenDatabase is a concrete implementation of TokenDatabase.
// It mimics the ChunkedTokenDatabase in the Python code.
type ChunkedTokenDatabase struct {
	chunkSize int
	metadata  LMCacheEngineMetadata
}

// NewChunkedTokenDatabase creates a new instance with the given config and metadata.
func NewChunkedTokenDatabase(config LMCacheEngineConfig, metadata LMCacheEngineMetadata) TokenProcessor {
	return &ChunkedTokenDatabase{
		chunkSize: config.ChunkSize,
		metadata:  metadata,
	}
}

// getInitHash returns the initial hash.
func (db *ChunkedTokenDatabase) getInitHash() string {
	return ""
}

// hash computes the SHA-256 hash of the concatenation of the prefixHash and the binary
// representation of the tokens slice. It returns the hex-encoded string.
func (db *ChunkedTokenDatabase) hash(tokens []uint32, prefixHash string) string {
	buf := new(bytes.Buffer)
	// write the prefixHash bytes (ASCII encoding)
	buf.WriteString(prefixHash)
	// write each token to the buffer as binary data (using 64-bit big-endian format)
	for _, token := range tokens {
		// convert token to int64 for binary consistency
		// LittleEndian is important to match the Python code
		if err := binary.Write(buf, binary.LittleEndian, int64(token)); err != nil {
			// In production code, you might handle this error appropriately.
			panic(err)
		}
	}
	sum := sha256.Sum256(buf.Bytes())
	return hex.EncodeToString(sum[:])
}

// chunkTokens splits the input slice of tokens into chunks of size chunkSize.
func (db *ChunkedTokenDatabase) chunkTokens(tokens []uint32) [][]uint32 {
	var chunks [][]uint32
	for i := 0; i < len(tokens); i += db.chunkSize {
		end := i + db.chunkSize
		if end > len(tokens) {
			end = len(tokens)
		}
		chunks = append(chunks, tokens[i:end])
	}
	return chunks
}

// prefixHashes computes the rolling (prefix) hash for each chunk and
// returns a slice of hash strings. It starts from the initial hash
// and then for each token chunk it computes the new hash.
func (db *ChunkedTokenDatabase) prefixHashes(tokenChunks [][]uint32) []string {
	prefixHash := db.getInitHash()
	hashes := make([]string, len(tokenChunks))
	for i, chunk := range tokenChunks {
		prefixHash = db.hash(chunk, prefixHash)
		hashes[i] = prefixHash
	}
	return hashes
}

// TokensToKVBlockKeys converts tokens into KVBlockKeys.
func (db *ChunkedTokenDatabase) TokensToKVBlockKeys(tokens []uint32, modelName string) []KVBlockKey {
	tokenChunks := db.chunkTokens(tokens)
	prefixHashes := db.prefixHashes(tokenChunks)

	return utils.SliceMap(prefixHashes, func(hashVal string) KVBlockKey {
		return KVBlockKey{
			Fmt:       db.metadata.Fmt,
			ModelName: modelName,
			WorldSize: db.metadata.WorldSize,
			WorkerID:  db.metadata.WorkerID,
			ChunkHash: hashVal,
		}
	})
}
