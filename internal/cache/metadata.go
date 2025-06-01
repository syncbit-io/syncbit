package cache

import (
	"sync"

	"syncbit/internal/core/types"
)

// BlockMetadata tracks which blocks are available for a file
type BlockMetadata struct {
	mu         sync.RWMutex
	TotalSize  types.Bytes
	BlockCount int
	HaveBlocks map[int]bool // Which blocks we have
}

// NewBlockMetadata creates metadata for a file of given size
func NewBlockMetadata(totalSize types.Bytes) *BlockMetadata {
	blockCount := int((totalSize + BlockSize - 1) / BlockSize) // Ceiling division
	return &BlockMetadata{
		TotalSize:  totalSize,
		BlockCount: blockCount,
		HaveBlocks: make(map[int]bool),
	}
}

// HasBlock returns true if we have the specified block
func (bm *BlockMetadata) HasBlock(blockIndex int) bool {
	bm.mu.RLock()
	defer bm.mu.RUnlock()
	return bm.HaveBlocks[blockIndex]
}

// SetBlock marks a block as available
func (bm *BlockMetadata) SetBlock(blockIndex int) {
	bm.mu.Lock()
	defer bm.mu.Unlock()
	bm.HaveBlocks[blockIndex] = true
}

// IsComplete returns true if we have all blocks
func (bm *BlockMetadata) IsComplete() bool {
	bm.mu.RLock()
	defer bm.mu.RUnlock()
	return len(bm.HaveBlocks) == bm.BlockCount
}

// GetMissingBlocks returns slice of missing block indices
func (bm *BlockMetadata) GetMissingBlocks() []int {
	bm.mu.RLock()
	defer bm.mu.RUnlock()

	var missing []int
	for i := range bm.BlockCount {
		if !bm.HaveBlocks[i] {
			missing = append(missing, i)
		}
	}
	return missing
}
