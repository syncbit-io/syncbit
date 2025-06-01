package cache

import (
	"syncbit/internal/core/types"
)

// BlockWriter provides an io.Writer interface that writes blocks as they arrive
type BlockWriter struct {
	cache    *BlockCache
	fileKey  string
	offset   types.Bytes
	totalSize types.Bytes
}

// NewBlockWriter creates a writer for streaming data into blocks
func (bc *BlockCache) NewBlockWriter(fileKey string, totalSize types.Bytes) *BlockWriter {
	bc.InitFile(fileKey, totalSize)
	return &BlockWriter{
		cache:     bc,
		fileKey:   fileKey,
		offset:    0,
		totalSize: totalSize,
	}
}

// Write implements io.Writer - writes directly into block buffers
func (bw *BlockWriter) Write(p []byte) (int, error) {
	err := bw.cache.WriteAt(bw.fileKey, bw.offset, p)
	if err != nil {
		return 0, err
	}

	bw.offset += types.Bytes(len(p))
	return len(p), nil
}

// WriteAt implements io.WriterAt - writes at specific offset
func (bw *BlockWriter) WriteAt(p []byte, off int64) (int, error) {
	err := bw.cache.WriteAt(bw.fileKey, types.Bytes(off), p)
	if err != nil {
		return 0, err
	}
	return len(p), nil
}

// Close finalizes the write operation
func (bw *BlockWriter) Close() error {
	// Nothing special needed - blocks are immediately available as written
	return nil
}
