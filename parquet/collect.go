package parquet

import (
	"context"
	"os"

	arrowhs "github.com/enviodev/hypersync-client-go/arrow"
	"github.com/enviodev/hypersync-client-go/logger"
	"github.com/enviodev/hypersync-client-go/options"
	"github.com/enviodev/hypersync-client-go/types"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// ArrowBatchFetcher is a function that fetches Arrow batches for a query.
type ArrowBatchFetcher func(ctx context.Context, query *types.Query) (*arrowhs.ArrowResponse, error)

// CollectConfig holds configuration for parquet collection.
type CollectConfig struct {
	// StreamOptions for controlling batch size, concurrency, etc.
	StreamOptions *options.StreamOptions
}

// DefaultCollectConfig returns default collection configuration.
func DefaultCollectConfig() *CollectConfig {
	return &CollectConfig{
		StreamOptions: options.DefaultStreamOptions(),
	}
}

// Collect streams data from HyperSync and writes it to Parquet files.
// The path should be a directory where parquet files will be created.
// Creates: blocks.parquet, transactions.parquet, logs.parquet, traces.parquet
func Collect(
	ctx context.Context,
	fetcher ArrowBatchFetcher,
	query *types.Query,
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

	toBlock := query.ToBlock
	currentQuery := *query

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		resp, err := fetcher(ctx, &currentQuery)
		if err != nil {
			return errors.Wrap(err, "failed to fetch arrow batches")
		}

		if err := writeBatches(writers, resp); err != nil {
			resp.Batches.Release()
			return errors.Wrap(err, "failed to write batches to parquet")
		}

		logger.L().Debug(
			"wrote parquet data",
			zap.Uint64("next_block", resp.NextBlock.Uint64()),
			zap.Uint64("response_size", resp.ResponseSize),
			zap.Int("blocks", len(resp.Batches.Blocks)),
			zap.Int("transactions", len(resp.Batches.Transactions)),
			zap.Int("logs", len(resp.Batches.Logs)),
			zap.Int("traces", len(resp.Batches.Traces)),
		)

		resp.Batches.Release()

		if resp.NextBlock.Cmp(toBlock) >= 0 {
			break
		}

		currentQuery.FromBlock = resp.NextBlock
	}

	return nil
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
