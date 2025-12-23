package hypersyncgo

import (
	"context"

	"github.com/terminally-online/hypersync-client-go/options"
	"github.com/terminally-online/hypersync-client-go/types"
	"github.com/pkg/errors"
)

// GetEvents executes a query and returns an EventResponse with logs joined to
// their associated blocks and transactions. This is a convenience method that
// wraps Get() and performs the join automatically.
func (c *Client) GetEvents(ctx context.Context, query *types.Query) (*types.EventResponse, error) {
	resp, err := c.Get(ctx, query)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get events")
	}

	return types.ToEventResponse(resp), nil
}

// CollectEvents fetches all data for a query, handling pagination automatically,
// and returns a single EventResponse with all logs joined to their associated
// blocks and transactions.
func (c *Client) CollectEvents(ctx context.Context, query *types.Query) (*types.EventResponse, error) {
	resp, err := c.Collect(ctx, query)
	if err != nil {
		return nil, errors.Wrap(err, "failed to collect events")
	}

	return types.ToEventResponse(resp), nil
}

// EventStream wraps a Stream and provides EventResponse instead of QueryResponse.
type EventStream struct {
	stream  *Stream
	eventCh chan *types.EventResponse
	errCh   chan error
	done    chan struct{}
}

// StreamEvents creates a streaming query that returns EventResponse objects
// with logs joined to their associated blocks and transactions.
func (c *Client) StreamEvents(ctx context.Context, query *types.Query, opts *options.StreamOptions) (*EventStream, error) {
	stream, err := c.Stream(ctx, query, opts)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create event stream")
	}

	eventStream := &EventStream{
		stream:  stream,
		eventCh: make(chan *types.EventResponse, opts.Concurrency.Uint64()),
		errCh:   make(chan error, opts.Concurrency.Uint64()),
		done:    make(chan struct{}),
	}

	go eventStream.run()

	return eventStream, nil
}

// run converts QueryResponse to EventResponse and forwards to the event channel.
func (es *EventStream) run() {
	defer close(es.eventCh)

	for {
		select {
		case resp, ok := <-es.stream.Channel():
			if !ok {
				return
			}
			eventResp := types.ToEventResponse(resp)
			es.eventCh <- eventResp
		case err := <-es.stream.Err():
			es.errCh <- err
		case <-es.stream.Done():
			close(es.done)
			return
		}
	}
}

// Channel returns the channel that receives EventResponse objects.
func (es *EventStream) Channel() <-chan *types.EventResponse {
	return es.eventCh
}

// Err returns the error channel.
func (es *EventStream) Err() <-chan error {
	return es.errCh
}

// Done returns a channel that signals when the stream is complete.
func (es *EventStream) Done() <-chan struct{} {
	return es.done
}

// Ack acknowledges that an EventResponse has been processed.
func (es *EventStream) Ack() {
	es.stream.Ack()
}

// Unsubscribe stops the event stream and releases resources.
func (es *EventStream) Unsubscribe() error {
	return es.stream.Unsubscribe()
}
