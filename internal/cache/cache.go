package cache

import (
	"fmt"
	"io"
	"sync"

	"syncbit/internal/config"
	"syncbit/internal/core/types"
)

// Cache provides a file-level cache with LRU eviction for efficient P2P file synchronization
type Cache struct {
	mu  sync.RWMutex
	cfg config.CacheConfig

	// Dataset and file organization
	datasets map[string]*DataSet // dataset name -> dataset

	// File cache for in-memory acceleration (keyed by file key)
	fileCache map[string]*FileEntry // file key -> file entry in memory
	fileIndex map[string]*FileIndex // file key -> file metadata

	// LRU cache for intelligent eviction
	lruCache *LRUCache

	// Usage tracking
	ramUsage  types.Bytes // Current RAM usage
	diskUsage types.Bytes // Current disk usage

	// File storage backend - stores complete files
	fileStorage FileStorage
}

// NewCache creates a new cache instance with the specified configuration
func NewCache(cfg config.CacheConfig, basePath string) (*Cache, error) {
	// Initialize file storage
	fileStorage, err := NewDiskFileStorage(basePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create file storage: %w", err)
	}

	// Initialize LRU cache with RAM limit
	lruCache := NewLRUCache(cfg.RAMLimit)

	cache := &Cache{
		cfg:         cfg,
		datasets:    make(map[string]*DataSet),
		fileCache:   make(map[string]*FileEntry),
		fileIndex:   make(map[string]*FileIndex),
		lruCache:    lruCache,
		ramUsage:    0,
		diskUsage:   0,
		fileStorage: fileStorage,
	}

	return cache, nil
}

// AddDataset adds a new dataset to the cache
func (c *Cache) AddDataset(dataset *DataSet) (*DataSet, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.datasets[dataset.Name()]; exists {
		return nil, fmt.Errorf("dataset %s already exists", dataset.Name())
	}

	c.datasets[dataset.Name()] = dataset
	return dataset, nil
}

// GetDataset retrieves a dataset by name
func (c *Cache) GetDataset(name string) (*DataSet, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if dataset, exists := c.datasets[name]; exists {
		return dataset, nil
	}

	return nil, fmt.Errorf("dataset %s not found", name)
}

// StoreFile stores a complete file in the cache
func (c *Cache) StoreFile(dataset, filepath string, data []byte) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	fileKey := GetFileKey(dataset, filepath)
	fileSize := types.Bytes(len(data))

	// Check if we need to evict files to make room in RAM
	if c.ramUsage+fileSize > c.cfg.RAMLimit {
		if err := c.evictFiles(fileSize); err != nil {
			return "", fmt.Errorf("failed to evict files: %w", err)
		}
	}

	// Store in memory cache
	fileEntry := NewFileEntryWithData(data)
	c.fileCache[fileKey] = fileEntry
	c.ramUsage += fileSize

	// Create/update file index entry
	fileIdx := NewFileIndex(dataset, filepath, fileKey, fileSize)
	fileIdx.MarkComplete()
	c.fileIndex[fileKey] = fileIdx

	// Update LRU cache tracking
	c.lruCache.Add(fileKey, fileSize)

	// Store to actual file on disk
	if err := c.fileStorage.WriteFile(dataset, filepath, data); err != nil {
		// Remove from memory cache if disk write fails
		delete(c.fileCache, fileKey)
		delete(c.fileIndex, fileKey)
		c.ramUsage -= fileSize
		c.lruCache.Remove(fileKey)
		return "", fmt.Errorf("failed to write file to disk: %w", err)
	}

	return fileKey, nil
}

// GetFile retrieves a complete file by its file key
func (c *Cache) GetFile(fileKey string) ([]byte, error) {
	c.mu.RLock()

	// Try memory cache first
	if fileEntry, exists := c.fileCache[fileKey]; exists {
		c.mu.RUnlock()

		// Update LRU cache tracking (requires write lock)
		c.mu.Lock()
		c.lruCache.Access(fileKey, types.Bytes(len(fileEntry.Data)))
		c.mu.Unlock()

		return fileEntry.Read()
	}
	c.mu.RUnlock()

	// Try reading from actual file on disk
	fileIdx, exists := func() (*FileIndex, bool) {
		c.mu.RLock()
		defer c.mu.RUnlock()
		idx, ok := c.fileIndex[fileKey]
		return idx, ok
	}()

	if exists {
		data, err := c.fileStorage.ReadFile(fileIdx.Dataset, fileIdx.FilePath)
		if err == nil {
			// Load back into memory cache if there's room
			c.mu.Lock()
			if c.ramUsage+types.Bytes(len(data)) <= c.cfg.RAMLimit {
				fileEntry := NewFileEntryWithData(data)
				c.fileCache[fileKey] = fileEntry
				c.ramUsage += types.Bytes(len(data))

				// Update LRU cache tracking
				c.lruCache.Add(fileKey, types.Bytes(len(data)))
			}
			c.mu.Unlock()

			return data, nil
		}
	}

	return nil, fmt.Errorf("file not found: %s", fileKey)
}

// GetFileByPath retrieves a file by its dataset and path
func (c *Cache) GetFileByPath(dataset, filepath string) ([]byte, error) {
	fileKey := GetFileKey(dataset, filepath)
	return c.GetFile(fileKey)
}

// HasFile checks if a file exists in the cache or on disk
func (c *Cache) HasFile(fileKey string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Check memory cache
	if _, exists := c.fileCache[fileKey]; exists {
		return true
	}

	// Check if we have metadata for this file
	if fileIdx, exists := c.fileIndex[fileKey]; exists && fileIdx.Complete {
		// Verify it exists on disk
		return c.fileStorage.FileExists(fileIdx.Dataset, fileIdx.FilePath)
	}

	return false
}

// HasFileByPath checks if a file exists by dataset and path
func (c *Cache) HasFileByPath(dataset, filepath string) bool {
	fileKey := GetFileKey(dataset, filepath)
	return c.HasFile(fileKey)
}

// evictFiles removes files using LRU eviction strategy to make room
func (c *Cache) evictFiles(targetSize types.Bytes) error {
	spaceNeeded := targetSize

	for spaceNeeded > 0 {
		leastValuable, hasAny := c.lruCache.GetLeastValuable()
		if !hasAny {
			break // No more files to evict
		}

		if fileEntry, exists := c.fileCache[leastValuable]; exists {
			fileSize := types.Bytes(len(fileEntry.Data))
			delete(c.fileCache, leastValuable)
			c.ramUsage -= fileSize
			spaceNeeded -= fileSize
			c.lruCache.Remove(leastValuable)
		} else {
			// Entry exists in LRU but not in fileCache - remove from LRU
			c.lruCache.Remove(leastValuable)
		}
	}

	return nil
}

// EnsureFile creates a file with the specified size if it doesn't exist
func (c *Cache) EnsureFile(dataset, filepath string, fileSize types.Bytes) error {
	return c.fileStorage.EnsureFile(dataset, filepath, fileSize)
}

// GetCacheStats returns current cache statistics
func (c *Cache) GetCacheStats() CacheStats {
	c.mu.RLock()
	defer c.mu.RUnlock()

	lruStats := c.lruCache.GetStats()

	stats := CacheStats{
		RAMUsage:   c.ramUsage,
		RAMLimit:   c.cfg.RAMLimit,
		DiskUsage:  c.diskUsage,
		DiskLimit:  c.cfg.DiskLimit,
		FilesInRAM: len(c.fileCache),
		TotalFiles: len(c.fileIndex),
		Datasets:   len(c.datasets),
		LRUStats:   &lruStats,
	}

	return stats
}

// CacheStats provides insights into cache usage and performance
type CacheStats struct {
	RAMUsage   types.Bytes `json:"ram_usage"`
	RAMLimit   types.Bytes `json:"ram_limit"`
	DiskUsage  types.Bytes `json:"disk_usage"`
	DiskLimit  types.Bytes `json:"disk_limit"`
	FilesInRAM int         `json:"files_in_ram"`
	TotalFiles int         `json:"total_files"`
	Datasets   int         `json:"datasets"`
	LRUStats   *LRUStats   `json:"lru_stats"`
}

// StreamStore stores data from a stream directly into the cache system
func (c *Cache) StreamStore(dataset, filepath string, reader io.Reader, fileSize types.Bytes) error {
	// Read the entire file into memory first
	data := make([]byte, fileSize)
	totalRead := 0

	for totalRead < int(fileSize) {
		n, err := reader.Read(data[totalRead:])
		totalRead += n

		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("failed to read from stream: %w", err)
		}
	}

	// Trim data to actual size read
	if totalRead < int(fileSize) {
		data = data[:totalRead]
	}

	// Store the complete file
	_, err := c.StoreFile(dataset, filepath, data)
	return err
}

// GetFileKey generates a consistent key for file storage
func GetFileKey(dataset, filepath string) string {
	return fmt.Sprintf("%s/%s", dataset, filepath)
}
