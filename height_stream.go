package hypersyncgo

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
)

const (
	// SSE keepalive ping interval from server is 5s, so timeout after 3x
	sseReadTimeout = 15 * time.Second

	// Reconnection backoff settings
	initialReconnectDelay = 200 * time.Millisecond
	maxReconnectDelay     = 30 * time.Second
	reconnectMultiplier   = 2.0
)

// HeightEvent represents an event from the height SSE stream.
type HeightEvent struct {
	Height    *big.Int
	Connected bool
	Error     error
}

// HeightStream manages an SSE connection to the /height/sse endpoint.
type HeightStream struct {
	client   *Client
	ctx      context.Context
	cancel   context.CancelFunc
	eventCh  chan HeightEvent
	doneCh   chan struct{}
}

// StreamHeight connects to the HyperSync SSE endpoint and streams block heights
// as they become available. It automatically reconnects on disconnection with
// exponential backoff.
func (c *Client) StreamHeight(ctx context.Context) *HeightStream {
	streamCtx, cancel := context.WithCancel(ctx)

	hs := &HeightStream{
		client:  c,
		ctx:     streamCtx,
		cancel:  cancel,
		eventCh: make(chan HeightEvent, 16),
		doneCh:  make(chan struct{}),
	}

	go hs.run()

	return hs
}

// Channel returns the channel that receives height events.
func (hs *HeightStream) Channel() <-chan HeightEvent {
	return hs.eventCh
}

// Done returns a channel that's closed when the stream is stopped.
func (hs *HeightStream) Done() <-chan struct{} {
	return hs.doneCh
}

// Close stops the height stream and cleans up resources.
func (hs *HeightStream) Close() {
	hs.cancel()
}

func (hs *HeightStream) run() {
	defer close(hs.doneCh)
	defer close(hs.eventCh)

	consecutiveFailures := 0

	for {
		select {
		case <-hs.ctx.Done():
			return
		default:
		}

		err := hs.connect()
		if err != nil {
			if hs.ctx.Err() != nil {
				return
			}

			consecutiveFailures++
			hs.sendEvent(HeightEvent{Error: err})

			delay := hs.getReconnectDelay(consecutiveFailures)
			select {
			case <-time.After(delay):
				continue
			case <-hs.ctx.Done():
				return
			}
		}

		consecutiveFailures = 0
	}
}

func (hs *HeightStream) connect() error {
	url := hs.client.GeUrlFromNodeAndPath(hs.client.opts, "height", "sse")

	req, err := http.NewRequestWithContext(hs.ctx, http.MethodGet, url, nil)
	if err != nil {
		return errors.Wrap(err, "failed to create SSE request")
	}

	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Cache-Control", "no-cache")
	req.Header.Set("Connection", "keep-alive")

	if hs.client.opts.BearerToken != nil && *hs.client.opts.BearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+*hs.client.opts.BearerToken)
	}

	sseClient := &http.Client{
		Timeout: 0, // No timeout for SSE connections
	}

	resp, err := sseClient.Do(req)
	if err != nil {
		return errors.Wrap(err, "failed to connect to SSE endpoint")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("SSE endpoint returned status %d: %s", resp.StatusCode, string(body))
	}

	hs.sendEvent(HeightEvent{Connected: true})

	return hs.readEvents(resp.Body)
}

func (hs *HeightStream) readEvents(body io.Reader) error {
	reader := bufio.NewReader(body)

	var eventType string
	var eventData string

	for {
		select {
		case <-hs.ctx.Done():
			return hs.ctx.Err()
		default:
		}

		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				return errors.New("SSE connection closed by server")
			}
			return errors.Wrap(err, "failed to read SSE event")
		}

		line = strings.TrimSpace(line)

		if line == "" {
			if eventType != "" && eventData != "" {
				if err := hs.handleEvent(eventType, eventData); err != nil {
					return err
				}
			}
			eventType = ""
			eventData = ""
			continue
		}

		if strings.HasPrefix(line, ":") {
			continue
		}

		if strings.HasPrefix(line, "event:") {
			eventType = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
		} else if strings.HasPrefix(line, "data:") {
			eventData = strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		}
	}
}

func (hs *HeightStream) handleEvent(eventType, eventData string) error {
	switch eventType {
	case "height":
		height, err := strconv.ParseUint(eventData, 10, 64)
		if err != nil {
			return errors.Wrapf(err, "failed to parse height: %s", eventData)
		}
		hs.sendEvent(HeightEvent{Height: new(big.Int).SetUint64(height)})
	case "ping":
		// Ignore keepalive pings
	default:
		// Unknown event type, ignore
	}
	return nil
}

func (hs *HeightStream) sendEvent(event HeightEvent) {
	select {
	case hs.eventCh <- event:
	case <-hs.ctx.Done():
	}
}

func (hs *HeightStream) getReconnectDelay(consecutiveFailures int) time.Duration {
	if consecutiveFailures <= 0 {
		return initialReconnectDelay
	}

	delay := initialReconnectDelay
	for i := 1; i < consecutiveFailures; i++ {
		delay = time.Duration(float64(delay) * reconnectMultiplier)
		if delay > maxReconnectDelay {
			return maxReconnectDelay
		}
	}
	return delay
}
