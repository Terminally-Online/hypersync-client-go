package types

import (
	"math/big"
)

// Event represents a unified blockchain event that joins a Log with its
// associated Block and Transaction. This provides a convenient way to
// access all related data for an event log in a single structure.
type Event struct {
	// Transaction contains the transaction data if transaction fields were requested.
	// Will be nil if no transaction fields were selected.
	Transaction *Transaction `json:"transaction,omitempty"`
	// Block contains the block data if block fields were requested in the query.
	// Will be nil if no block fields were selected.
	Block *Block `json:"block,omitempty"`
	// Log is the actual event log. This is always present.
	Log Log `json:"log"`
}

// EventResponse contains the result of an events query, with logs joined
// to their associated blocks and transactions.
type EventResponse struct {
	// ArchiveHeight is the height of the archive at the time of the query.
	ArchiveHeight *big.Int `json:"archive_height,omitempty"`

	// NextBlock is the next block number to query for pagination.
	NextBlock *big.Int `json:"next_block"`

	// TotalExecutionTime is the total time spent executing the query on the server.
	TotalExecutionTime uint64 `json:"total_execution_time"`

	// Events contains the joined event data.
	Events []Event `json:"events"`

	// RollbackGuard contains information to detect chain reorganizations.
	RollbackGuard *RollbackGuard `json:"rollback_guard,omitempty"`
}

// JoinEvents takes a QueryResponse and joins the logs with their associated
// blocks and transactions, returning a slice of Events.
func JoinEvents(resp *QueryResponse) []Event {
	if resp == nil {
		return nil
	}

	// Build lookup maps for efficient joining
	blocksByNumber := make(map[uint64]*Block)
	for i := range resp.Data.Blocks {
		block := &resp.Data.Blocks[i]
		if block.Number != nil {
			blocksByNumber[block.Number.Uint64()] = block
		}
	}

	txsByHash := make(map[string]*Transaction)
	for i := range resp.Data.Transactions {
		tx := &resp.Data.Transactions[i]
		if tx.Hash != nil {
			txsByHash[tx.Hash.Hex()] = tx
		}
	}

	// Join logs with blocks and transactions
	events := make([]Event, 0, len(resp.Data.Logs))
	for _, log := range resp.Data.Logs {
		event := Event{
			Log: log,
		}

		// Join with block if available
		if log.BlockNumber != nil {
			if block, ok := blocksByNumber[log.BlockNumber.Uint64()]; ok {
				event.Block = block
			}
		}

		// Join with transaction if available
		if log.TransactionHash != nil {
			if tx, ok := txsByHash[log.TransactionHash.Hex()]; ok {
				event.Transaction = tx
			}
		}

		events = append(events, event)
	}

	return events
}

// ToEventResponse converts a QueryResponse to an EventResponse by joining
// logs with their associated blocks and transactions.
func ToEventResponse(resp *QueryResponse) *EventResponse {
	if resp == nil {
		return nil
	}

	return &EventResponse{
		ArchiveHeight:      resp.ArchiveHeight,
		NextBlock:          resp.NextBlock,
		TotalExecutionTime: resp.TotalExecutionTime,
		Events:             JoinEvents(resp),
		RollbackGuard:      resp.RollbackGuard,
	}
}
