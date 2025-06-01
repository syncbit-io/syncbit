package cache

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"syncbit/internal/core/types"
)

const BlockSize = 1024 * 1024 // 1MB blocks

// Global block buffer pool
var blockPool = &sync.Pool{
	New: func() any {
		buf := make([]byte, BlockSize)
		return &buf
	},
}

// DatasetCache represents the persistent cache metadata for a dataset
type DatasetCache struct {
	Name  string                    `json:"name"`
	Files map[string]*FileCacheInfo `json:"files"`
}

// FileCacheInfo represents the cache metadata for a single file
type FileCacheInfo struct {
	Size     int64             `json:"size"`
	Complete bool              `json:"complete"`
	Blocks   map[string]int64  `json:"blocks"` // blockIndex -> blockSize
	ModTime  *time.Time        `json:"modtime,omitempty"`
	ETag     string            `json:"etag,omitempty"`
}

// BlockCache manages file blocks using 1MB fixed-size buffers
// AND persists complete files to disk for direct access by other programs
type BlockCache struct {
	mu sync.RWMutex

	// Block storage: fileKey -> blockIndex -> data (for fast P2P access)
	blocks map[string]map[int][]byte

	// Metadata: fileKey -> block tracking info
	metadata map[string]*BlockMetadata

	// LRU cache for block eviction
	lruCache *LRUCache

	// Open file handles for writing complete files to disk
	openFiles map[string]*os.File

	// Disk storage
	basePath string
}

// NewBlockCache creates a new block-based cache
func NewBlockCache(ramLimit types.Bytes, basePath string) *BlockCache {
	return &BlockCache{
		blocks:    make(map[string]map[int][]byte),
		metadata:  make(map[string]*BlockMetadata),
		lruCache:  NewLRUCache(ramLimit),
		openFiles: make(map[string]*os.File),
		basePath:  basePath,
	}
}

// getFilePath returns the disk path for a complete file
func (bc *BlockCache) getFilePath(fileKey string) string {
	// Convert fileKey format "dataset:filepath" to "dataset/filepath"
	safeKey := strings.ReplaceAll(fileKey, ":", "/")
	return filepath.Join(bc.basePath, "files", safeKey)
}

// getBlockPath returns the disk path for a specific block (legacy, kept for compatibility)
func (bc *BlockCache) getBlockPath(fileKey string, blockIndex int) string {
	return filepath.Join(bc.basePath, "blocks", fileKey, fmt.Sprintf("%08d.block", blockIndex))
}

// getBlockKey creates a unique key for a block in the LRU cache
// Uses a more efficient format to avoid parsing overhead
func (bc *BlockCache) getBlockKey(fileKey string, blockIndex int) string {
	// Pre-allocate with estimated capacity to avoid string growth
	// Most fileKeys are short, blockIndex max ~4 digits
	key := make([]byte, 0, len(fileKey)+8)
	key = append(key, fileKey...)
	key = append(key, ':')
	key = strconv.AppendInt(key, int64(blockIndex), 10)
	return string(key)
}

// evictBlocksToMakeRoom evicts LRU blocks to make room for new blocks
func (bc *BlockCache) evictBlocksToMakeRoom(neededSize types.Bytes) error {
	for {
		// Check if we have enough space
		if bc.lruCache.usage+neededSize <= bc.lruCache.capacity {
			break
		}

		// Get least recently used block
		lruKey, exists := bc.lruCache.GetLeastValuable()
		if !exists {
			// No blocks to evict - this shouldn't happen
			return fmt.Errorf("no blocks available for eviction")
		}

		// Parse the LRU key to get fileKey and blockIndex
		// Format is "fileKey:blockIndex" - find last colon to avoid splitting fileKey
		colonPos := strings.LastIndex(lruKey, ":")
		if colonPos == -1 {
			return fmt.Errorf("invalid LRU key format: %s", lruKey)
		}

		fileKey := lruKey[:colonPos]
		blockIndex, err := strconv.Atoi(lruKey[colonPos+1:])
		if err != nil {
			return fmt.Errorf("failed to parse block index from LRU key %s: %w", lruKey, err)
		}

		// Remove from memory
		if fileBlocks, exists := bc.blocks[fileKey]; exists {
			if block, exists := fileBlocks[blockIndex]; exists {
				// Return buffer to pool
				blockPool.Put(&block)
				delete(fileBlocks, blockIndex)

				// Clean up empty file maps
				if len(fileBlocks) == 0 {
					delete(bc.blocks, fileKey)
				}
			}
		}

		// Remove from LRU
		bc.lruCache.Remove(lruKey)
	}

	return nil
}


// loadBlockFromDisk loads a block from the complete file on disk
func (bc *BlockCache) loadBlockFromDisk(fileKey string, blockIndex int) ([]byte, error) {
	filePath := bc.getFilePath(fileKey)

	// Check if complete file exists
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return nil, fmt.Errorf("file not found on disk: %s", fileKey)
	}

	// Open file for reading
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", filePath, err)
	}
	defer file.Close()

	// Calculate block offset and size
	offset := int64(blockIndex * BlockSize)
	
	// Determine block size (last block might be smaller)
	meta := bc.metadata[fileKey] // Already have read lock from caller
	blockSize := BlockSize
	if blockIndex == meta.BlockCount-1 {
		// Last block might be smaller
		lastBlockSize := int(meta.TotalSize % BlockSize)
		if lastBlockSize > 0 {
			blockSize = lastBlockSize
		}
	}

	// Read block data from file at offset
	blockData := make([]byte, blockSize)
	n, err := file.ReadAt(blockData, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read block %d from file: %w", blockIndex, err)
	}

	// Return only the data we actually read
	return blockData[:n], nil
}

// InitFile prepares a file for block-based operations
func (bc *BlockCache) InitFile(fileKey string, totalSize types.Bytes) {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	if _, exists := bc.metadata[fileKey]; !exists {
		meta := NewBlockMetadata(totalSize)
		bc.metadata[fileKey] = meta
		bc.blocks[fileKey] = make(map[int][]byte)

		// Scan disk for existing blocks
		bc.scanExistingBlocks(fileKey, meta)
	}
}

// getCacheFilePath returns the path to the dataset cache file
func (bc *BlockCache) getCacheFilePath(datasetName string) string {
	return filepath.Join(bc.basePath, "files", datasetName, ".syncbit-cache.json")
}

// loadDatasetCache loads cache metadata for a dataset
func (bc *BlockCache) loadDatasetCache(datasetName string) (*DatasetCache, error) {
	cachePath := bc.getCacheFilePath(datasetName)
	
	// Check if cache file exists
	if _, err := os.Stat(cachePath); os.IsNotExist(err) {
		// Create new empty cache
		return &DatasetCache{
			Name:  datasetName,
			Files: make(map[string]*FileCacheInfo),
		}, nil
	}

	// Read and parse cache file
	data, err := os.ReadFile(cachePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read cache file: %w", err)
	}

	var cache DatasetCache
	if err := json.Unmarshal(data, &cache); err != nil {
		return nil, fmt.Errorf("failed to parse cache file: %w", err)
	}

	return &cache, nil
}

// saveDatasetCache saves cache metadata for a dataset
func (bc *BlockCache) saveDatasetCache(datasetName string, cache *DatasetCache) error {
	cachePath := bc.getCacheFilePath(datasetName)
	
	// Ensure directory exists
	dir := filepath.Dir(cachePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create cache directory: %w", err)
	}

	// Marshal to JSON with nice formatting
	data, err := json.MarshalIndent(cache, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal cache: %w", err)
	}

	// Write atomically using a temp file
	tempPath := cachePath + ".tmp"
	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write temp cache file: %w", err)
	}

	// Atomic rename
	if err := os.Rename(tempPath, cachePath); err != nil {
		return fmt.Errorf("failed to rename cache file: %w", err)
	}

	return nil
}

// persistBlockCompletion updates cache metadata when a block is completed
func (bc *BlockCache) persistBlockCompletion(fileKey string, blockIndex, blockSize int) {
	// Extract dataset name and file path
	parts := strings.SplitN(fileKey, ":", 2)
	var datasetName, filePath string
	
	if len(parts) == 2 {
		// New format: "dataset:filepath"
		datasetName = parts[0]
		filePath = parts[1]
	} else {
		// Legacy format: just use the whole key as both dataset and file
		datasetName = "default"
		filePath = fileKey
	}

	// Load current cache (ignore errors - create new if needed)
	cache, err := bc.loadDatasetCache(datasetName)
	if err != nil {
		cache = &DatasetCache{
			Name:  datasetName,
			Files: make(map[string]*FileCacheInfo),
		}
	}

	// Ensure file entry exists
	if cache.Files[filePath] == nil {
		cache.Files[filePath] = &FileCacheInfo{
			Size:     int64(bc.metadata[fileKey].TotalSize),
			Complete: false,
			Blocks:   make(map[string]int64),
		}
	}

	// Add completed block
	cache.Files[filePath].Blocks[strconv.Itoa(blockIndex)] = int64(blockSize)

	// Save cache (ignore errors for now - this is async metadata)
	bc.saveDatasetCache(datasetName, cache)
}

// persistFileCompletion updates cache metadata when a file is completed
func (bc *BlockCache) persistFileCompletion(fileKey string) {
	// Extract dataset name and file path
	parts := strings.SplitN(fileKey, ":", 2)
	var datasetName, filePath string
	
	if len(parts) == 2 {
		// New format: "dataset:filepath"
		datasetName = parts[0]
		filePath = parts[1]
	} else {
		// Legacy format: just use the whole key as both dataset and file
		datasetName = "default"
		filePath = fileKey
	}

	// Load current cache
	cache, err := bc.loadDatasetCache(datasetName)
	if err != nil {
		return // If we can't load cache, skip persistence
	}

	// Update file completion status
	if fileInfo, exists := cache.Files[filePath]; exists {
		fileInfo.Complete = true
		now := time.Now()
		fileInfo.ModTime = &now
	}

	// Save cache
	bc.saveDatasetCache(datasetName, cache)
}

// scanExistingBlocks loads cache metadata and restores block state  
func (bc *BlockCache) scanExistingBlocks(fileKey string, meta *BlockMetadata) {
	// Extract dataset name from fileKey 
	parts := strings.SplitN(fileKey, ":", 2)
	var datasetName, filePath string
	
	if len(parts) == 2 {
		// New format: "dataset:filepath"
		datasetName = parts[0]
		filePath = parts[1]
	} else {
		// Legacy format: just use the whole key as both dataset and file
		datasetName = "default"
		filePath = fileKey
	}

	// Load dataset cache
	cache, err := bc.loadDatasetCache(datasetName)
	if err != nil {
		// If we can't load cache, fall back to file scanning
		bc.scanFileOnDisk(fileKey, meta)
		return
	}

	// Check if we have cached info for this file
	if fileInfo, exists := cache.Files[filePath]; exists {
		if fileInfo.Complete && fileInfo.Size == int64(meta.TotalSize) {
			// File is marked complete in cache - mark all blocks as available
			for blockIndex := 0; blockIndex < meta.BlockCount; blockIndex++ {
				meta.SetBlock(blockIndex)
			}
		} else {
			// File is partial - restore individual block state
			for blockIndexStr := range fileInfo.Blocks {
				if blockIndex, err := strconv.Atoi(blockIndexStr); err == nil {
					if blockIndex < meta.BlockCount {
						meta.SetBlock(blockIndex)
					}
				}
			}
		}
	} else {
		// No cached info - fall back to file scanning
		bc.scanFileOnDisk(fileKey, meta)
	}
}

// scanFileOnDisk is a fallback that checks if complete file exists on disk
func (bc *BlockCache) scanFileOnDisk(fileKey string, meta *BlockMetadata) {
	filePath := bc.getFilePath(fileKey)

	// Check if complete file exists
	if fileInfo, err := os.Stat(filePath); err == nil {
		// File exists, check if it's the expected size
		if types.Bytes(fileInfo.Size()) == meta.TotalSize {
			// File is complete - mark all blocks as available
			for blockIndex := 0; blockIndex < meta.BlockCount; blockIndex++ {
				meta.SetBlock(blockIndex)
			}
		}
	}
}

// ensureFileOpen ensures a file is open for writing
func (bc *BlockCache) ensureFileOpen(fileKey string) (*os.File, error) {
	if file, exists := bc.openFiles[fileKey]; exists {
		return file, nil
	}

	filePath := bc.getFilePath(fileKey)
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory %s: %w", dir, err)
	}

	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", filePath, err)
	}

	bc.openFiles[fileKey] = file
	return file, nil
}

// WriteAt writes data at the specified offset, filling blocks as needed
// AND writes the data to the actual file on disk at the correct offset
func (bc *BlockCache) WriteAt(fileKey string, offset types.Bytes, data []byte) error {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	meta, exists := bc.metadata[fileKey]
	if !exists {
		return fmt.Errorf("file not initialized: %s", fileKey)
	}

	// First, write to the actual file on disk at the correct offset
	file, err := bc.ensureFileOpen(fileKey)
	if err != nil {
		return fmt.Errorf("failed to open target file: %w", err)
	}

	if _, err := file.WriteAt(data, int64(offset)); err != nil {
		return fmt.Errorf("failed to write to file at offset %d: %w", offset, err)
	}

	dataOffset := 0
	currentOffset := offset

	for dataOffset < len(data) {
		blockIndex := int(currentOffset / BlockSize)
		blockOffset := currentOffset % BlockSize

		// Get or create block
		fileBlocks := bc.blocks[fileKey]
		if fileBlocks == nil {
			fileBlocks = make(map[int][]byte)
			bc.blocks[fileKey] = fileBlocks
		}
		block, exists := fileBlocks[blockIndex]
		if !exists {
			// Make room if needed
			if err := bc.evictBlocksToMakeRoom(BlockSize); err != nil {
				return fmt.Errorf("failed to make room for block: %w", err)
			}

			// Get buffer from pool
			block = *blockPool.Get().(*[]byte)
			// Reset to full capacity
			block = block[:BlockSize]
			fileBlocks[blockIndex] = block

			// Add to LRU cache
			blockKey := bc.getBlockKey(fileKey, blockIndex)
			bc.lruCache.Add(blockKey, BlockSize)
		} else {
			// Block exists - mark as accessed in LRU
			blockKey := bc.getBlockKey(fileKey, blockIndex)
			bc.lruCache.Access(blockKey, BlockSize)
		}

		// Calculate how much to write to this block
		remainingInBlock := BlockSize - int(blockOffset)
		remainingData := len(data) - dataOffset
		writeSize := remainingInBlock
		if writeSize > remainingData {
			writeSize = remainingData
		}

		// Write data directly into block buffer
		copy(block[blockOffset:blockOffset+types.Bytes(writeSize)], data[dataOffset:dataOffset+writeSize])

		// If this completes the block, mark it as available
		expectedBlockSize := BlockSize
		if blockIndex == meta.BlockCount-1 {
			// Last block might be smaller
			lastBlockSize := int(meta.TotalSize % BlockSize)
			if lastBlockSize > 0 {
				expectedBlockSize = lastBlockSize
			}
		}

		// Check if we've written up to the expected size for this block
		if blockOffset+types.Bytes(writeSize) >= types.Bytes(expectedBlockSize) {
			// Block is complete - mark it as available for P2P transfers
			meta.SetBlock(blockIndex)
			
			// Persist cache metadata when block completes
			bc.persistBlockCompletion(fileKey, blockIndex, expectedBlockSize)
		}

		// Check if the entire file is complete
		if meta.IsComplete() {
			// Mark file as complete in cache
			bc.persistFileCompletion(fileKey)
			
			// Close the file handle since we're done writing
			if file, exists := bc.openFiles[fileKey]; exists {
				file.Close()
				delete(bc.openFiles, fileKey)
			}
		}

		// Move to next chunk
		dataOffset += writeSize
		currentOffset += types.Bytes(writeSize)
	}

	return nil
}

// ReadBlock returns a copy of the specified block
func (bc *BlockCache) ReadBlock(fileKey string, blockIndex int) ([]byte, error) {
	bc.mu.RLock()
	defer bc.mu.RUnlock()

	meta, exists := bc.metadata[fileKey]
	if !exists {
		return nil, fmt.Errorf("file not found: %s", fileKey)
	}

	if !meta.HasBlock(blockIndex) {
		return nil, fmt.Errorf("block %d not available for file %s", blockIndex, fileKey)
	}

	fileBlocks := bc.blocks[fileKey]
	block, exists := fileBlocks[blockIndex]
	if !exists {
		// Try to load from disk
		diskData, err := bc.loadBlockFromDisk(fileKey, blockIndex)
		if err != nil {
			return nil, fmt.Errorf("block %d not available: %w", blockIndex, err)
		}

		// Always cache the block (evict LRU if needed)
		if err := bc.evictBlocksToMakeRoom(BlockSize); err != nil {
			return nil, fmt.Errorf("failed to make room for block: %w", err)
		}

		// Get buffer from pool and copy data
		cacheBlock := *blockPool.Get().(*[]byte)
		cacheBlock = cacheBlock[:BlockSize]
		copy(cacheBlock, diskData)

		if fileBlocks == nil {
			bc.blocks[fileKey] = make(map[int][]byte)
			fileBlocks = bc.blocks[fileKey]
		}
		fileBlocks[blockIndex] = cacheBlock

		// Add to LRU cache (this block is now most recently used)
		blockKey := bc.getBlockKey(fileKey, blockIndex)
		bc.lruCache.Add(blockKey, BlockSize)

		block = cacheBlock
	} else {
		// Cache hit - mark as accessed in LRU
		blockKey := bc.getBlockKey(fileKey, blockIndex)
		bc.lruCache.Access(blockKey, BlockSize)
	}

	// Determine actual block size
	blockSize := BlockSize
	if blockIndex == meta.BlockCount-1 {
		// Last block might be smaller
		lastBlockSize := int(meta.TotalSize % BlockSize)
		if lastBlockSize > 0 {
			blockSize = lastBlockSize
		}
	}

	// Return copy of actual data
	result := make([]byte, blockSize)
	copy(result, block[:blockSize])
	return result, nil
}

// HasBlock checks if a block is available (in memory or on disk)
func (bc *BlockCache) HasBlock(fileKey string, blockIndex int) bool {
	bc.mu.RLock()
	meta, exists := bc.metadata[fileKey]
	bc.mu.RUnlock()

	if !exists {
		return false
	}

	// Check metadata first (fast path)
	if meta.HasBlock(blockIndex) {
		return true
	}

	// If not in metadata, check if it exists on disk
	blockPath := bc.getBlockPath(fileKey, blockIndex)
	if _, err := os.Stat(blockPath); err == nil {
		// Update metadata to reflect disk presence
		bc.mu.Lock()
		meta.SetBlock(blockIndex)
		bc.mu.Unlock()
		return true
	}

	return false
}

// GetFileMetadata returns metadata for a file
func (bc *BlockCache) GetFileMetadata(fileKey string) (*BlockMetadata, bool) {
	bc.mu.RLock()
	defer bc.mu.RUnlock()

	meta, exists := bc.metadata[fileKey]
	return meta, exists
}

// ReadFile assembles and returns the complete file if all blocks are available
func (bc *BlockCache) ReadFile(fileKey string) ([]byte, error) {
	bc.mu.RLock()
	defer bc.mu.RUnlock()

	meta, exists := bc.metadata[fileKey]
	if !exists {
		return nil, fmt.Errorf("file not found: %s", fileKey)
	}

	if !meta.IsComplete() {
		return nil, fmt.Errorf("file incomplete: %s", fileKey)
	}

	result := make([]byte, meta.TotalSize)

	for blockIndex := 0; blockIndex < meta.BlockCount; blockIndex++ {
		// Use ReadBlock to handle both memory and disk cases with read-through caching
		blockData, err := bc.ReadBlock(fileKey, blockIndex)
		if err != nil {
			return nil, fmt.Errorf("failed to read block %d: %w", blockIndex, err)
		}

		// Calculate offset in result buffer
		offset := blockIndex * BlockSize
		copy(result[offset:], blockData)
	}

	return result, nil
}

// GetAvailableBlocks returns a list of block indices that are available for the given file
func (bc *BlockCache) GetAvailableBlocks(fileKey string) []int {
	bc.mu.RLock()
	defer bc.mu.RUnlock()
	
	fileBlocks, exists := bc.blocks[fileKey]
	if !exists {
		return nil
	}
	
	var blocks []int
	for blockIndex := range fileBlocks {
		blocks = append(blocks, blockIndex)
	}
	
	// Sort for consistent ordering
	sort.Ints(blocks)
	return blocks
}
