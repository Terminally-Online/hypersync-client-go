package streams

import (
	"context"
	"math/big"
	"sync"

	errorshs "github.com/enviodev/hypersync-client-go/errors"
	"github.com/enviodev/hypersync-client-go/logger"
	"github.com/enviodev/hypersync-client-go/options"
	"github.com/enviodev/hypersync-client-go/types"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// QueryDescriptor wraps a Query with its generation for batch size tracking.
type QueryDescriptor struct {
	Query      *types.Query
	Generation uint32
}

// WorkerFn defines a function type that takes a QueryDescriptor and returns a response.
type WorkerFn func(descriptor QueryDescriptor) (*types.QueryResponse, error)

// Worker processes query descriptors concurrently and manages batch size adjustment.
type Worker struct {
	ctx      context.Context
	opts     *options.StreamOptions
	iterator *BlockIterator
	done     chan struct{}
	result   chan OrderedResult
	channel  chan *types.QueryResponse
	ackWg    sync.WaitGroup
	wg       sync.WaitGroup
}

// OrderedResult holds the result of processing a descriptor, including its index,
// generation, response, and any error encountered.
type OrderedResult struct {
	index      int
	generation uint32
	record     *types.QueryResponse
	err        error
}

// NewWorker creates a new Worker instance.
func NewWorker(ctx context.Context, iterator *BlockIterator, channel chan *types.QueryResponse, done chan struct{}, opts *options.StreamOptions) (*Worker, error) {
	return &Worker{
		ctx:      ctx,
		opts:     opts,
		iterator: iterator,
		channel:  channel,
		done:     done,
		result:   make(chan OrderedResult, big.NewInt(0).Mul(opts.Concurrency, big.NewInt(10)).Uint64()),
	}, nil
}

// Start begins the worker's operation using the provided WorkerFn and a channel of descriptors.
func (w *Worker) Start(workerFn WorkerFn, descriptor <-chan QueryDescriptor) error {
	g, ctx := errgroup.WithContext(w.ctx)

	// Create an indexed channel to preserve order
	type indexedDescriptor struct {
		index      int
		generation uint32
		query      *types.Query
	}
	indexedChan := make(chan indexedDescriptor)

	// Goroutine to index descriptors
	go func() {
		index := 0
		for entry := range descriptor {
			indexedChan <- indexedDescriptor{
				index:      index,
				generation: entry.Generation,
				query:      entry.Query,
			}
			index++
		}
		close(indexedChan)
	}()

	// Start worker goroutines
	for workerId := uint64(0); workerId < w.opts.Concurrency.Uint64(); workerId++ {
		g.Go(func() error {
			for {
				select {
				case <-ctx.Done():
					return nil
				case <-w.done:
					return nil
				case entry, ok := <-indexedChan:
					if !ok {
						return nil
					}

					w.wg.Add(1)
					resp, err := workerFn(QueryDescriptor{Query: entry.query, Generation: entry.generation})
					w.result <- OrderedResult{
						index:      entry.index,
						generation: entry.generation,
						record:     resp,
						err:        err,
					}
				}
			}
		})
	}

	// Collect results in order and publish them to the output channel
	g.Go(func() error {
		type pendingResult struct {
			record     *types.QueryResponse
			generation uint32
		}
		results := make(map[int]pendingResult)
		nextIndex := 0
		nextGeneration := uint32(0)

	mainLoop:
		for res := range w.result {
			if res.err != nil {
				logger.L().Error(
					"error processing stream entry",
					zap.Error(res.err),
					zap.Any("processing_index", res.index),
				)
				w.wg.Done()
				continue
			}
			results[res.index] = pendingResult{record: res.record, generation: res.generation}
			w.wg.Done()

			// Push results to the output channel in order
			for {
				if pending, ok := results[nextIndex]; ok {
					record := pending.record

					// Adjust batch size based on response size if this generation matches
					if pending.generation == nextGeneration {
						nextGeneration++
						w.adjustBatchSize(record.ResponseSize, nextGeneration)
					}

					if !w.opts.DisableAcknowledgements {
						w.ackWg.Add(1)
					}
					w.channel <- record
					delete(results, nextIndex)

					// Check if this is the last record to process
					if record.NextBlock.Cmp(w.iterator.GetEndAsBigInt()) == 0 {
						break mainLoop
					}

					nextIndex++
				} else {
					break
				}
			}
		}

		// Wait for all messages to be processed
		w.wg.Wait()

		// Close the result channel to signal completion
		close(w.result)

		return errorshs.ErrWorkerCompleted
	})

	// Wait for all goroutines to finish
	if err := g.Wait(); err != nil && !errors.Is(err, errorshs.ErrWorkerCompleted) {
		return err
	}

	return w.Stop()
}

// adjustBatchSize dynamically adjusts the batch size based on response size.
func (w *Worker) adjustBatchSize(responseSize uint64, newGeneration uint32) {
	if w.opts.ResponseBytesCeiling == 0 || w.opts.ResponseBytesFloor == 0 {
		return
	}

	step := *w.iterator.GetStep()
	currentBatchSize, _ := unpackStep(step)

	if responseSize > w.opts.ResponseBytesCeiling {
		// Response too large, decrease batch size
		ratio := float64(w.opts.ResponseBytesCeiling) / float64(responseSize)
		newBatchSize := uint32(float64(currentBatchSize) * ratio)
		if w.opts.MinBatchSize != nil {
			minBatch := uint32(w.opts.MinBatchSize.Uint64())
			if newBatchSize < minBatch {
				newBatchSize = minBatch
			}
		}
		if newBatchSize != currentBatchSize {
			w.iterator.UpdateBatchSize(newBatchSize, newGeneration)
			logger.L().Debug(
				"decreased batch size due to large response",
				zap.Uint64("response_size", responseSize),
				zap.Uint32("old_batch_size", currentBatchSize),
				zap.Uint32("new_batch_size", newBatchSize),
			)
		}
	} else if responseSize < w.opts.ResponseBytesFloor {
		// Response too small, increase batch size
		ratio := float64(w.opts.ResponseBytesFloor) / float64(responseSize)
		newBatchSize := uint32(float64(currentBatchSize) * ratio)
		if w.opts.MaxBatchSize != nil {
			maxBatch := uint32(w.opts.MaxBatchSize.Uint64())
			if newBatchSize > maxBatch {
				newBatchSize = maxBatch
			}
		}
		if newBatchSize != currentBatchSize {
			w.iterator.UpdateBatchSize(newBatchSize, newGeneration)
			logger.L().Debug(
				"increased batch size due to small response",
				zap.Uint64("response_size", responseSize),
				zap.Uint32("old_batch_size", currentBatchSize),
				zap.Uint32("new_batch_size", newBatchSize),
			)
		}
	}
}

// AddPendingAck increments the acknowledgment wait group.
func (w *Worker) AddPendingAck() {
	w.ackWg.Add(1)
}

// Ack acknowledges that a response has been processed.
func (w *Worker) Ack() {
	if !w.opts.DisableAcknowledgements {
		w.ackWg.Done()
	}
}

// Done returns a channel that can be used to signal when the worker's operations are done.
func (w *Worker) Done() <-chan struct{} {
	return w.done
}

// Stop stops the worker's operations and waits for all goroutines to complete.
func (w *Worker) Stop() error {
	// Wait for all acknowledgments before closing done channel
	w.ackWg.Wait()
	w.wg.Wait()
	close(w.done)
	return nil
}
