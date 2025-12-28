package parquet

import (
	"os"

	arrowhs "github.com/terminally-online/hypersync-client-go/arrow"
	"github.com/terminally-online/hypersync-client-go/options"
	"github.com/pkg/errors"
)

// CollectProgress holds progress information for the collection process.
type CollectProgress struct {
	CurrentBlock uint64
	TargetBlock  uint64
	ResponseSize uint64
	Blocks       int
	Transactions int
	Logs         int
	Traces       int
}

// ProgressCallback is called after each batch is written with progress info.
type ProgressCallback func(progress CollectProgress)

// CollectConfig holds configuration for parquet collection.
type CollectConfig struct {
	StreamOptions *options.StreamOptions
	OnProgress    ProgressCallback
}

// DefaultCollectConfig returns default collection configuration.
func DefaultCollectConfig() *CollectConfig {
	return &CollectConfig{
		StreamOptions: options.DefaultStreamOptions(),
	}
}

// ArrowStreamInterface defines the interface for consuming an Arrow stream.
type ArrowStreamInterface interface {
	Channel() <-chan *arrowhs.ArrowResponse
	Done() <-chan struct{}
	Ack()
	Err() <-chan error
	Unsubscribe() error
}

// Collect consumes Arrow batches from a stream and writes them to Parquet files.
// The path should be a directory where parquet files will be created.
// Creates: blocks.parquet, transactions.parquet, logs.parquet, traces.parquet
func Collect(
	stream ArrowStreamInterface,
	targetBlock uint64,
	path string,
	config *CollectConfig,
) error {
	if config == nil {
		config = DefaultCollectConfig()
	}

	if err := os.MkdirAll(path, 0755); err != nil {
		return errors.Wrap(err, "failed to create parquet directory")
	}

	writers := NewDataWriters(path)
	defer writers.Close()

	for {
		select {
		case err := <-stream.Err():
			return errors.Wrap(err, "stream error during parquet collection")

		case resp, ok := <-stream.Channel():
			if !ok {
				return nil
			}

			if err := writeBatches(writers, resp); err != nil {
				resp.Batches.Release()
				return errors.Wrap(err, "failed to write batches to parquet")
			}

			if config.OnProgress != nil {
				config.OnProgress(CollectProgress{
					CurrentBlock: resp.NextBlock.Uint64(),
					TargetBlock:  targetBlock,
					ResponseSize: resp.ResponseSize,
					Blocks:       len(resp.Batches.Blocks),
					Transactions: len(resp.Batches.Transactions),
					Logs:         len(resp.Batches.Logs),
					Traces:       len(resp.Batches.Traces),
				})
			}

			resp.Batches.Release()
			stream.Ack()

			if resp.NextBlock.Uint64() >= targetBlock {
				return nil
			}

		case <-stream.Done():
			return nil
		}
	}
}

func writeBatches(writers *DataWriters, resp *arrowhs.ArrowResponse) error {
	for _, batch := range resp.Batches.Blocks {
		if err := writers.Blocks.Write(batch); err != nil {
			return errors.Wrap(err, "failed to write blocks batch")
		}
	}

	for _, batch := range resp.Batches.Transactions {
		if err := writers.Transactions.Write(batch); err != nil {
			return errors.Wrap(err, "failed to write transactions batch")
		}
	}

	for _, batch := range resp.Batches.Logs {
		if err := writers.Logs.Write(batch); err != nil {
			return errors.Wrap(err, "failed to write logs batch")
		}
	}

	for _, batch := range resp.Batches.Traces {
		if err := writers.Traces.Write(batch); err != nil {
			return errors.Wrap(err, "failed to write traces batch")
		}
	}

	return nil
}
