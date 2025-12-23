package streams

import (
	"math/big"
	"sync/atomic"
)

// BlockIterator manages the iteration over a range of blocks, supporting batching and
// thread-safe updates to the current offset. The step field packs both the batch size
// (lower 32 bits) and generation counter (upper 32 bits) into a single uint64 for
// atomic operations.
type BlockIterator struct {
	offset uint64  // current offset in the block range
	end    uint64  // end of the block range
	step   *uint64 // packed: lower 32 bits = batch size, upper 32 bits = generation
}

// NewBlockIterator creates a new BlockIterator with the specified offset, end, and batch size.
// The step is stored as a packed uint64 with batch size in lower 32 bits and generation (0) in upper 32 bits.
func NewBlockIterator(offset uint64, end uint64, batchSize uint64) *BlockIterator {
	step := batchSize // generation starts at 0, so upper 32 bits are 0
	return &BlockIterator{
		offset: offset,
		end:    end,
		step:   &step,
	}
}

// GetCurrentOffset returns the current offset in the block range.
func (b *BlockIterator) GetCurrentOffset() uint64 {
	return b.offset
}

// GetEnd returns the end of the block range.
func (b *BlockIterator) GetEnd() uint64 {
	return b.end
}

// GetEndAsBigInt returns the end of the block range as a big.Int.
func (b *BlockIterator) GetEndAsBigInt() *big.Int {
	return big.NewInt(0).SetUint64(b.end)
}

// Completed checks if the iteration has reached or passed the end of the block range.
func (b *BlockIterator) Completed() bool {
	return b.offset >= b.end
}

// GetStep returns the current step pointer for atomic operations.
func (b *BlockIterator) GetStep() *uint64 {
	return b.step
}

// unpackStep extracts batch size and generation from the packed step value.
func unpackStep(step uint64) (batchSize uint32, generation uint32) {
	batchSize = uint32(step)        // lower 32 bits
	generation = uint32(step >> 32) // upper 32 bits
	return
}

// packStep combines batch size and generation into a packed step value.
func packStep(batchSize uint32, generation uint32) uint64 {
	return uint64(batchSize) | (uint64(generation) << 32)
}

// Next returns the next batch of blocks to be processed. It updates the current offset and
// returns the start, end, generation, and a boolean indicating if the operation was successful.
// The generation is used to track batch size changes and prevent duplicate adjustments.
func (b *BlockIterator) Next() (start uint64, end uint64, generation uint32, ok bool) {
	if b.offset >= b.end {
		return 0, 0, 0, false
	}

	start = b.offset
	step := atomic.LoadUint64(b.step)
	batchSize, generation := unpackStep(step)

	b.offset = min(b.offset+uint64(batchSize), b.end)
	return start, b.offset, generation, true
}

// UpdateBatchSize atomically updates the batch size with a new generation.
// This is used for dynamic batch size adjustment based on response sizes.
func (b *BlockIterator) UpdateBatchSize(newBatchSize uint32, newGeneration uint32) {
	newStep := packStep(newBatchSize, newGeneration)
	atomic.StoreUint64(b.step, newStep)
}
