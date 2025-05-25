package cache

import (
	"fmt"
	"sync"
)

type FileEntry struct {
	mu     sync.RWMutex
	path   string
	size   int64
	blocks map[int]*BlockEntry // block index -> block entry
}

func NewFileEntry(path string, size int64) *FileEntry {
	return &FileEntry{
		path:   path,
		size:   size,
		blocks: make(map[int]*BlockEntry, GetBlocks(size)),
	}
}

func (f *FileEntry) Path() string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.path
}

func (f *FileEntry) Size() int64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.size
}

func (f *FileEntry) AddBlock(index int, block *BlockEntry) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.blocks[index] = block
}

func (f *FileEntry) GetBlock(index int) (*BlockEntry, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	block, ok := f.blocks[index]
	if !ok {
		return nil, fmt.Errorf("block not found: %d", index)
	}
	return block, nil
}

func (f *FileEntry) Write(index int, data []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	block, ok := f.blocks[index]
	if !ok {
		return fmt.Errorf("block not found: %d", index)
	}
	return block.Write(data)
}

func (f *FileEntry) Read(index int) ([]byte, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	block, ok := f.blocks[index]
	if !ok {
		return nil, fmt.Errorf("block not found: %d", index)
	}
	return block.Read()
}
