package tokenization

import (
	"context"
	"sync"

	"k8s.io/client-go/util/workqueue"
)

// Task represents a unit of work for tokenizing a prompt.
type Task struct {
	Prompt    string
	ModelName string
}

// Pool encapsulates the queue, worker pool, and token indexer.
type Pool struct {
	workers int
	queue   workqueue.TypedRateLimitingInterface[Task]
	wg      sync.WaitGroup

	indexer   Indexer
	tokenizer Tokenizer // TODO: replace with map of active tokenizers
}

// NewTokenizationPool initializes a TokenizationPool with the specified number
// of workers and the provided Indexer.
func NewTokenizationPool(workers int, store Indexer) *Pool {
	return &Pool{
		workers:   workers,
		queue:     workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[Task]()),
		indexer:   store,
		tokenizer: NewHFTokenizer(),
	}
}

// AddTask enqueues a new tokenization task.
// This method only enqueues the task and does not start processing it.
func (pool *Pool) AddTask(prompt, modelName string) {
	task := Task{
		Prompt:    prompt,
		ModelName: modelName,
	}
	pool.queue.Add(task)
}

// Run launches worker goroutines that process tasks until the context is
// cancelled.
func (pool *Pool) Run(ctx context.Context) {
	for i := 0; i < pool.workers; i++ {
		pool.wg.Add(1)
		go pool.workerLoop(i)
	}

	<-ctx.Done()

	pool.queue.ShutDown()
	pool.wg.Wait()
}

// workerLoop is the main processing loop for each worker.
func (pool *Pool) workerLoop(workerID int) {
	defer pool.wg.Done()
	for {
		task, shutdown := pool.queue.Get()
		if shutdown {
			return
		}

		// Process the task.
		if err := pool.processTask(task); err == nil {
			pool.queue.Forget(task)
		} else {
			pool.queue.AddRateLimited(task)
		}
		pool.queue.Done(task)
	}
}

// processTask tokenizes the prompt, extracts a prefix, and updates the
// indexer.
func (pool *Pool) processTask(task Task) error {
	tokenIds, offsets, err := pool.tokenizer.Encode(task.Prompt, task.ModelName)
	if err != nil {
		return err
	}

	pool.indexer.AddFullTokenization(task.ModelName, task.Prompt, tokenIds, offsets)

	return nil
}
