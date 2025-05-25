package cache

import (
	"fmt"
	"sync"
	"time"
)

const (
	BlockSize = 1024 * 1024 // 1MB
)

func GetBlocks(size int64) int {
	blocks := int(size / BlockSize)
	if size % BlockSize != 0 {
		blocks++
	}
	if blocks == 0 {
		blocks = 1
	}
	return blocks
}

type BlockEntry struct {
	mu        sync.RWMutex
	Size      int64
	Timestamp time.Time
	Data      []byte
}

func NewBlockEntry() *BlockEntry {
	return &BlockEntry{
		Size:      BlockSize,
		Timestamp: time.Now(),
		Data:      make([]byte, BlockSize),
	}
}

func (b *BlockEntry) Length() int64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return int64(len(b.Data))
}

func (b *BlockEntry) Write(data []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(data) > len(b.Data) {
		return fmt.Errorf("data too large: %d > %d", len(data), len(b.Data))
	}
	copy(b.Data, data)
	b.Timestamp = time.Now()
	return nil
}

func (b *BlockEntry) Read() ([]byte, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.Data, nil
}
