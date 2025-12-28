package hypersyncgo

import (
	"context"
	"math/big"
	"sync"

	arrowhs "github.com/terminally-online/hypersync-client-go/arrow"
	"github.com/terminally-online/hypersync-client-go/options"
	"github.com/terminally-online/hypersync-client-go/streams"
	"github.com/terminally-online/hypersync-client-go/types"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

// ArrowStream represents a streaming process that outputs raw Arrow batches
// for efficient parquet writing. It uses the same concurrent worker infrastructure
// as Stream but preserves Arrow RecordBatches instead of decoding to Go structs.
type ArrowStream struct {
	ctx           context.Context
	cancelFn      context.CancelFunc
	client        *Client
	queryCh       chan streams.QueryDescriptor
	ch            chan *arrowhs.ArrowResponse
	errCh         chan error
	opts          *options.StreamOptions
	query         *types.Query
	iterator      *streams.BlockIterator
	worker        *streams.Worker[*arrowhs.ArrowResponse]
	done          chan struct{}
	mu            *sync.RWMutex
	unsubOnce     sync.Once
}

// NewArrowStream creates a new ArrowStream instance with the provided context, client, query, and options.
func NewArrowStream(ctx context.Context, client *Client, query *types.Query, opts *options.StreamOptions) (*ArrowStream, error) {
	if vErr := opts.Validate(); vErr != nil {
		return nil, errors.Wrap(vErr, "failed to validate stream options")
	}

	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	ch := make(chan *arrowhs.ArrowResponse, opts.Concurrency.Uint64())
	blockIter := streams.NewBlockIterator(query.FromBlock.Uint64(), query.ToBlock.Uint64(), opts.BatchSize.Uint64())

	meta := streams.ResponseMeta[*arrowhs.ArrowResponse]{
		GetResponseSize: func(r *arrowhs.ArrowResponse) uint64 { return r.ResponseSize },
		GetNextBlock:    func(r *arrowhs.ArrowResponse) *big.Int { return r.NextBlock },
	}

	worker, err := streams.NewWorker(ctx, blockIter, ch, done, opts, meta)
	if err != nil {
		cancel()
		return nil, errors.Wrap(err, "failed to create new arrow stream worker")
	}

	return &ArrowStream{
		ctx:      ctx,
		opts:     opts,
		client:   client,
		cancelFn: cancel,
		query:    query,
		iterator: blockIter,
		worker:   worker,
		queryCh:  make(chan streams.QueryDescriptor, opts.Concurrency.Uint64()),
		ch:       ch,
		errCh:    make(chan error, opts.Concurrency.Uint64()),
		done:     done,
		mu:       &sync.RWMutex{},
	}, nil
}

// ProcessNextQuery processes the next query using the client and returns the Arrow response.
// It loops until the entire requested block range is fetched, handling pagination.
func (s *ArrowStream) ProcessNextQuery(descriptor streams.QueryDescriptor) (*arrowhs.ArrowResponse, error) {
	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()
	return s.runQueryToEnd(ctx, descriptor.Query)
}

// runQueryToEnd fetches data for a query, handling pagination if the server returns partial results.
// It accumulates all Arrow batches into a single ArrowResponse and tracks total response size.
func (s *ArrowStream) runQueryToEnd(ctx context.Context, query *types.Query) (*arrowhs.ArrowResponse, error) {
	toBlock := query.ToBlock
	currentQuery := *query

	var combinedResponse *arrowhs.ArrowResponse
	var totalResponseSize uint64

	for {
		resp, err := s.client.GetArrowBatches(ctx, &currentQuery)
		if err != nil {
			if combinedResponse != nil {
				combinedResponse.Batches.Release()
			}
			return nil, err
		}

		totalResponseSize += resp.ResponseSize

		if combinedResponse == nil {
			combinedResponse = resp
		} else {
			combinedResponse.Batches.Blocks = append(combinedResponse.Batches.Blocks, resp.Batches.Blocks...)
			combinedResponse.Batches.Transactions = append(combinedResponse.Batches.Transactions, resp.Batches.Transactions...)
			combinedResponse.Batches.Logs = append(combinedResponse.Batches.Logs, resp.Batches.Logs...)
			combinedResponse.Batches.Traces = append(combinedResponse.Batches.Traces, resp.Batches.Traces...)
			combinedResponse.NextBlock = resp.NextBlock
			combinedResponse.ArchiveHeight = resp.ArchiveHeight
			if resp.RollbackGuard != nil {
				combinedResponse.RollbackGuard = resp.RollbackGuard
			}
		}

		if resp.NextBlock.Cmp(toBlock) >= 0 {
			break
		}

		currentQuery.FromBlock = resp.NextBlock
	}

	combinedResponse.ResponseSize = totalResponseSize
	return combinedResponse, nil
}

// Subscribe starts the streaming process, initializing the first query and handling subsequent ones.
func (s *ArrowStream) Subscribe() error {
	g, ctx := errgroup.WithContext(s.ctx)

	response, err := s.client.GetArrowBatches(ctx, s.query)
	if err != nil {
		return err
	}

	if !s.opts.DisableAcknowledgements {
		s.worker.AddPendingAck()
	}
	s.ch <- response

	if response.NextBlock.Cmp(s.query.ToBlock) >= 0 {
		s.worker.Stop()
		return nil
	}

	g.Go(func() error {
		return s.worker.Start(s.ProcessNextQuery, s.queryCh)
	})

	go func() {
		for {
			start, end, generation, ok := s.iterator.Next()
			if !ok {
				break
			}

			iQuery := *s.query
			iQuery.FromBlock = new(big.Int).SetUint64(start)
			iQuery.ToBlock = new(big.Int).SetUint64(end)
			s.queryCh <- streams.QueryDescriptor{
				Query:      &iQuery,
				Generation: generation,
			}

			if s.iterator.Completed() {
				return
			}
		}
	}()

	select {
	case <-s.done:
		return nil
	case <-ctx.Done():
		return s.ctx.Err()
	default:
		if wErr := g.Wait(); wErr != nil {
			return wErr
		}
		return nil
	}
}

// Unsubscribe stops the stream and closes all channels associated with it.
// This method is idempotent and safe to call multiple times.
func (s *ArrowStream) Unsubscribe() error {
	s.unsubOnce.Do(func() {
		s.cancelFn()
		s.worker.Stop()
		close(s.queryCh)
		close(s.ch)
		close(s.errCh)
	})
	return nil
}

// QueueError adds an error to the stream's error channel.
func (s *ArrowStream) QueueError(err error) {
	s.errCh <- err
}

// Err returns the stream's error channel.
func (s *ArrowStream) Err() <-chan error {
	return s.errCh
}

// Channel returns the stream's Arrow response channel.
func (s *ArrowStream) Channel() <-chan *arrowhs.ArrowResponse {
	return s.ch
}

// Ack acknowledges that a response has been processed.
func (s *ArrowStream) Ack() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.worker.Ack()
}

// Done returns a channel that signals when the stream is done.
func (s *ArrowStream) Done() <-chan struct{} {
	return s.worker.Done()
}
