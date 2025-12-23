package arrowhs

import (
	"bytes"
	"io"
	"math/big"

	"capnproto.org/go/capnp/v3"
	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/ipc"
	hypersynccapnp "github.com/terminally-online/hypersync-client-go/capnp"
	"github.com/terminally-online/hypersync-client-go/types"
	"github.com/ethereum/go-ethereum/common"
	"github.com/pkg/errors"
)

// ArrowBatches holds raw Arrow RecordBatches for each data type.
type ArrowBatches struct {
	Blocks       []arrow.Record
	Transactions []arrow.Record
	Logs         []arrow.Record
	Traces       []arrow.Record
}

// ArrowResponse contains the query metadata and raw Arrow batches.
type ArrowResponse struct {
	ArchiveHeight      *big.Int
	NextBlock          *big.Int
	TotalExecutionTime uint64
	RollbackGuard      *types.RollbackGuard
	Batches            ArrowBatches
	ResponseSize       uint64
}

// ReadArrowBatches reads a capnp response and extracts raw Arrow RecordBatches
// without converting to Go structs. This is optimized for Parquet writing.
func ReadArrowBatches(bReader io.ReadCloser, responseSize uint64) (*ArrowResponse, error) {
	decoder := capnp.NewPackedDecoder(bReader)
	msg, err := decoder.Decode()
	if err != nil {
		return nil, errors.Wrap(err, "failed to decode packed message")
	}

	queryResponse, err := hypersynccapnp.ReadRootQueryResponse(msg)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get root pointer")
	}

	response := &ArrowResponse{
		ResponseSize: responseSize,
	}

	if queryResponse.ArchiveHeight() != -1 {
		response.ArchiveHeight = big.NewInt(queryResponse.ArchiveHeight())
	}

	response.NextBlock = big.NewInt(0).SetUint64(queryResponse.NextBlock())
	response.TotalExecutionTime = queryResponse.TotalExecutionTime()

	if queryResponse.HasRollbackGuard() {
		rg, rgErr := queryResponse.RollbackGuard()
		if rgErr != nil {
			return nil, errors.Wrap(rgErr, "failed to get rollback guard")
		}

		hash, hErr := rg.Hash()
		if hErr != nil {
			return nil, errors.Wrap(hErr, "failed to get rollback guard hash")
		}

		firstParentHash, fphErr := rg.FirstParentHash()
		if fphErr != nil {
			return nil, errors.Wrap(fphErr, "failed to get rollback guard first parent hash")
		}

		response.RollbackGuard = &types.RollbackGuard{
			BlockNumber:      big.NewInt(0).SetUint64(rg.BlockNumber()),
			Timestamp:        rg.Timestamp(),
			Hash:             common.BytesToHash(hash),
			FirstBlockNumber: rg.FirstBlockNumber(),
			FirstParentHash:  common.BytesToHash(firstParentHash),
		}
	}

	dataPtr, dpErr := queryResponse.Data()
	if dpErr != nil {
		return nil, errors.Wrap(dpErr, "failed to read query response data")
	}

	if dataPtr.HasBlocks() {
		data, err := dataPtr.Blocks()
		if err != nil {
			return nil, errors.Wrap(err, "failed to parse block data")
		}
		batches, err := extractBatches(data)
		if err != nil {
			return nil, errors.Wrap(err, "failed to extract block batches")
		}
		response.Batches.Blocks = batches
	}

	if dataPtr.HasTransactions() {
		data, err := dataPtr.Transactions()
		if err != nil {
			return nil, errors.Wrap(err, "failed to parse transaction data")
		}
		batches, err := extractBatches(data)
		if err != nil {
			return nil, errors.Wrap(err, "failed to extract transaction batches")
		}
		response.Batches.Transactions = batches
	}

	if dataPtr.HasLogs() {
		data, err := dataPtr.Logs()
		if err != nil {
			return nil, errors.Wrap(err, "failed to parse log data")
		}
		batches, err := extractBatches(data)
		if err != nil {
			return nil, errors.Wrap(err, "failed to extract log batches")
		}
		response.Batches.Logs = batches
	}

	if dataPtr.HasTraces() {
		data, err := dataPtr.Traces()
		if err != nil {
			return nil, errors.Wrap(err, "failed to parse trace data")
		}
		batches, err := extractBatches(data)
		if err != nil {
			return nil, errors.Wrap(err, "failed to extract trace batches")
		}
		response.Batches.Traces = batches
	}

	return response, nil
}

// extractBatches reads Arrow IPC data and returns the raw RecordBatches.
func extractBatches(data []byte) ([]arrow.Record, error) {
	if len(data) < 16 {
		return nil, nil
	}

	reader := bytes.NewBuffer(data[8:])
	arrowReader, err := ipc.NewReader(reader)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create arrow reader")
	}
	defer arrowReader.Release()

	var batches []arrow.Record
	for arrowReader.Next() {
		rec := arrowReader.Record()
		if rec == nil {
			break
		}
		rec.Retain()
		batches = append(batches, rec)
	}

	if err := arrowReader.Err(); err != nil {
		return nil, errors.Wrap(err, "error reading arrow data")
	}

	return batches, nil
}

// Release releases all Arrow records in the batches.
func (ab *ArrowBatches) Release() {
	for _, r := range ab.Blocks {
		r.Release()
	}
	for _, r := range ab.Transactions {
		r.Release()
	}
	for _, r := range ab.Logs {
		r.Release()
	}
	for _, r := range ab.Traces {
		r.Release()
	}
}
