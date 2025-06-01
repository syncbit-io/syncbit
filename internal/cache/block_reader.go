package cache

import (
	"fmt"
	"io"

	"syncbit/internal/core/types"
)

// BlockReader provides an io.Reader interface for reading from blocks
type BlockReader struct {
	cache      *BlockCache
	fileKey    string
	offset     types.Bytes
	totalSize  types.Bytes
}

// NewBlockReader creates a reader for streaming data from blocks
func (bc *BlockCache) NewBlockReader(fileKey string) (*BlockReader, error) {
	meta, exists := bc.GetFileMetadata(fileKey)
	if !exists {
		return nil, fmt.Errorf("file not found: %s", fileKey)
	}

	return &BlockReader{
		cache:     bc,
		fileKey:   fileKey,
		offset:    0,
		totalSize: meta.TotalSize,
	}, nil
}

// Read implements io.Reader
func (br *BlockReader) Read(p []byte) (int, error) {
	if br.offset >= br.totalSize {
		return 0, io.EOF
	}

	blockIndex := int(br.offset / BlockSize)
	blockOffset := br.offset % BlockSize

	// Check if this block is available
	if !br.cache.HasBlock(br.fileKey, blockIndex) {
		return 0, fmt.Errorf("block %d not available", blockIndex)
	}

	// Read from the block
	blockData, err := br.cache.ReadBlock(br.fileKey, blockIndex)
	if err != nil {
		return 0, err
	}

	// Copy what we can from this block
	available := len(blockData) - int(blockOffset)
	needed := len(p)
	remaining := int(br.totalSize - br.offset)

	toCopy := min(min(available, needed), remaining)

	copy(p, blockData[blockOffset:blockOffset+types.Bytes(toCopy)])
	br.offset += types.Bytes(toCopy)

	return toCopy, nil
}

// Close finalizes the read operation
func (br *BlockReader) Close() error {
	return nil
}
