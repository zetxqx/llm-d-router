package client

import (
	"context"
	"k8s.io/klog/v2"

	"fmt"
	"github.com/neuralmagic/distributed-kv-cache/pkg/kvindex"
	"github.com/neuralmagic/distributed-kv-cache/pkg/tokenization"
	"os"

	"github.com/redis/go-redis/v9"
)

// PodScore couples a pod identifier (IP) with a score.
type PodScore struct {
	Name  string
	Score float64
}

type Config struct {
	kvindex.LMCacheEngineConfig
	kvindex.LMCacheEngineMetadata

	ScoringStrategy KVScoringStrategy
}

type KVCacheIndexer struct {
	tokensIndexer   tokenization.Indexer   // gets tokens for a prompt
	tokensProcessor kvindex.TokenProcessor // turns tokens to kv block keys
	kvBlockIndexer  kvindex.KVBlockIndexer // looks up pods for block keys
	kvBlockScorer   KVBlockScorer          // scores pods based on block hits

	tokenizersPool *tokenization.Pool
}

// NewKVCacheIndexer creates a KVCacheIndexer with default scorer and config.
func NewKVCacheIndexer(cfg Config) (*KVCacheIndexer, error) {
	scorer, _ := NewKVBlockScorer(cfg.ScoringStrategy)

	// TODO: move somewhere else
	redisClient := redis.NewClient(&redis.Options{
		Addr:     os.Getenv("REDIS_HOST"),
		Password: os.Getenv("REDIS_PASSWORD"), // no password set
		DB:       0,                           // use default DB
	})

	_, err := redisClient.Ping(context.Background()).Result()
	if err != nil {
		return nil, fmt.Errorf("could not connect to Redis: %v", err)
	}

	tokensIndexer := tokenization.NewContainedTokenStore()
	return &KVCacheIndexer{
		tokensIndexer:   tokensIndexer,
		tokensProcessor: kvindex.NewChunkedTokenDatabase(cfg.LMCacheEngineConfig, cfg.LMCacheEngineMetadata),
		kvBlockIndexer:  kvindex.NewRedisKVBlockIndexer(redisClient),
		kvBlockScorer:   scorer,
		tokenizersPool:  tokenization.NewTokenizationPool(5, tokensIndexer),
	}, nil
}

// Run starts the indexer.
func (k *KVCacheIndexer) Run(ctx context.Context) {
	k.tokenizersPool.Run(ctx)
}

func (k *KVCacheIndexer) GetPodScores(ctx context.Context, prompt, modelName string) ([]PodScore, error) {
	logger := klog.FromContext(ctx).WithName("kvcache-indexer")
	/*// 0. add to tokenizers pool
	k.tokenizersPool.AddTask(prompt, modelName)

	// 1. get available tokens of longest prefix
	tokens := k.tokensIndexer.FindLongestContainedTokens(prompt, modelName)
	if len(tokens) == 0 {
		return nil, nil
	}*/

	tokens, _, err := tokenization.NewHFTokenizer().Encode(prompt, modelName)
	if err != nil {
		return nil, fmt.Errorf("failed to encode prompt: %w", err)
	}

	// 2. get block keys
	blockKeys := k.tokensProcessor.TokensToKVBlockKeys(tokens, modelName)
	logger.Info("made block keys", "blockKeys", blockKeys)

	// 3. query kvblock indexer for pods
	strBlockKeys, keyToPods, err := k.kvBlockIndexer.GetPodsForKeys(ctx, blockKeys)
	if err != nil {
		return nil, fmt.Errorf("failed to query kvblock indexer: %w", err)
	}
	logger.Info("queried kvblock indexer", "strBlockKeys", strBlockKeys, "keyToPods", keyToPods)

	// 4. score pods
	podScores, err := k.kvBlockScorer.Score(strBlockKeys, keyToPods)
	if err != nil {
		return nil, fmt.Errorf("failed to query kvblock scorer: %w", err)
	}

	return podScores, nil
}
