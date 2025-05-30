package cache

import (
	"fmt"
	"io"
	"sync"
	"time"

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

	// Metadata manager for persistent storage of cache state
	metadataManager *MetadataManager

	// Performance metrics
	metrics *CacheMetrics
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

	// Initialize metadata manager
	metadataManager := NewMetadataManager(basePath)

	cache := &Cache{
		cfg:             cfg,
		datasets:        make(map[string]*DataSet),
		fileCache:       make(map[string]*FileEntry),
		fileIndex:       make(map[string]*FileIndex),
		lruCache:        lruCache,
		ramUsage:        0,
		diskUsage:       0,
		fileStorage:     fileStorage,
		metadataManager: metadataManager,
		metrics:         NewCacheMetrics(),
	}

	// Load existing metadata if available
	if err := cache.metadataManager.LoadMetadata(); err != nil {
		return nil, fmt.Errorf("failed to load cache metadata: %w", err)
	}

	// Restore cache state from metadata rather than scanning disk
	if err := cache.restoreFromMetadata(); err != nil {
		return nil, fmt.Errorf("failed to restore cache from metadata: %w", err)
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

// StoreFile stores a complete file in the cache and returns the file key
func (c *Cache) StoreFile(dataset, filepath string, data []byte) (string, error) {
	fileKey := GetFileKey(dataset, filepath)
	fileSize := types.Bytes(len(data))

	c.mu.Lock()
	defer c.mu.Unlock()

	// Create dataset if it doesn't exist
	if _, exists := c.datasets[dataset]; !exists {
		c.datasets[dataset] = NewDataSet(dataset)
	}

	// Check if we need to evict files to make room in RAM
	if c.ramUsage+fileSize > c.cfg.RAMLimit {
		if err := c.evictFiles(fileSize); err != nil {
			c.metrics.RecordWriteError()
			return "", fmt.Errorf("failed to evict files: %w", err)
		}
	}

	// Store in memory cache
	fileEntry := NewFileEntryWithData(data)
	c.fileCache[fileKey] = fileEntry
	c.ramUsage += fileSize

	// Add file to dataset
	c.datasets[dataset].AddFile(filepath, fileEntry)

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
		c.metrics.RecordWriteError()
		return "", fmt.Errorf("failed to write file to disk: %w", err)
	}

	// Create and save metadata for the file
	fileMeta := &FileMetadata{
		Dataset:   dataset,
		FilePath:  filepath,
		FileKey:   fileKey,
		Size:      fileSize,
		ModTime:   time.Now(),
		ETag:      "", // TODO: Calculate ETag if needed
		Timestamp: time.Now(),
		Complete:  true,
		OnDisk:    true,
	}
	c.metadataManager.AddFileMetadata(fileKey, fileMeta)

	// Save metadata to disk (async to avoid blocking)
	go func() {
		if err := c.metadataManager.SaveMetadata(); err != nil {
			// Log error but don't fail the operation
			// TODO: Add proper logging
		}
	}()

	// Record successful write
	c.metrics.RecordWrite(fileSize)
	return fileKey, nil
}

// GetFile retrieves a complete file by its file key
func (c *Cache) GetFile(fileKey string) ([]byte, error) {
	startTime := time.Now()

	c.mu.RLock()

	// Try memory cache first
	if fileEntry, exists := c.fileCache[fileKey]; exists {
		c.mu.RUnlock()

		// Update LRU cache tracking (requires write lock)
		c.mu.Lock()
		c.lruCache.Access(fileKey, types.Bytes(len(fileEntry.Data)))
		c.mu.Unlock()

		data, err := fileEntry.Read()
		if err == nil {
			// Record cache hit
			duration := time.Since(startTime)
			c.metrics.RecordCacheHit(duration, types.Bytes(len(data)))
		} else {
			c.metrics.RecordReadError()
		}
		return data, err
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

			// Record cache hit (from disk)
			duration := time.Since(startTime)
			c.metrics.RecordCacheHit(duration, types.Bytes(len(data)))
			return data, nil
		} else {
			c.metrics.RecordReadError()
		}
	}

	// Record cache miss
	duration := time.Since(startTime)
	c.metrics.RecordCacheMiss(duration, 0)
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

			// Record eviction
			c.metrics.RecordEviction(fileSize)
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
	perfStats := c.metrics.GetStats()
	throughputStats := perfStats.GetThroughputStats()

	stats := CacheStats{
		RAMUsage:        c.ramUsage,
		RAMLimit:        c.cfg.RAMLimit,
		DiskUsage:       c.diskUsage,
		DiskLimit:       c.cfg.DiskLimit,
		FilesInRAM:      len(c.fileCache),
		TotalFiles:      len(c.fileIndex),
		Datasets:        len(c.datasets),
		LRUStats:        &lruStats,
		PerfStats:       &perfStats,
		ThroughputStats: &throughputStats,
	}

	return stats
}

// GetMetrics returns the performance metrics tracker
func (c *Cache) GetMetrics() *CacheMetrics {
	return c.metrics
}

// ResetMetrics resets all performance metrics
func (c *Cache) ResetMetrics() {
	c.metrics.Reset()
}

// CacheStats provides insights into cache usage and performance
type CacheStats struct {
	RAMUsage        types.Bytes            `json:"ram_usage"`
	RAMLimit        types.Bytes            `json:"ram_limit"`
	DiskUsage       types.Bytes            `json:"disk_usage"`
	DiskLimit       types.Bytes            `json:"disk_limit"`
	FilesInRAM      int                    `json:"files_in_ram"`
	TotalFiles      int                    `json:"total_files"`
	Datasets        int                    `json:"datasets"`
	LRUStats        *LRUStats              `json:"lru_stats"`
	PerfStats       *CachePerformanceStats `json:"perf_stats"`
	ThroughputStats *ThroughputStats       `json:"throughput_stats"`
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

// GetFileKey generates a unique key for a file within a dataset
func GetFileKey(dataset, filepath string) string {
	return fmt.Sprintf("%s/%s", dataset, filepath)
}

// ListDatasets returns a list of all dataset names in the cache
func (c *Cache) ListDatasets() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	datasets := make([]string, 0, len(c.datasets))
	for name := range c.datasets {
		datasets = append(datasets, name)
	}
	return datasets
}

// ListFiles returns a list of all file paths in a specific dataset
func (c *Cache) ListFiles(datasetName string) []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	dataset, exists := c.datasets[datasetName]
	if !exists {
		return []string{}
	}

	files := make([]string, 0, len(dataset.files))
	for filepath := range dataset.files {
		files = append(files, filepath)
	}
	return files
}

// NewCacheReaderWriter creates a types.ReaderWriter that integrates with the cache system
// This provides read-through and write-through caching with rate limiting and progress tracking
func (c *Cache) NewCacheReaderWriter(dataset, filepath string, fileSize types.Bytes, opts ...types.RWOption) *types.ReaderWriter {
	// Create cache-backed reader and writer
	reader := c.NewFileReader(dataset, filepath, fileSize)
	writer := c.NewFileWriter(dataset, filepath, fileSize)

	// Add the cache reader and writer to the options
	allOpts := append(opts,
		types.RWWithIOReader(reader),
		types.RWWithIOWriter(writer),
	)

	return types.NewReaderWriter(allOpts...)
}

// NewCacheReader creates a types.ReaderWriter configured only for reading from cache
func (c *Cache) NewCacheReader(dataset, filepath string, fileSize types.Bytes, opts ...types.RWOption) *types.ReaderWriter {
	reader := c.NewFileReader(dataset, filepath, fileSize)

	allOpts := append(opts, types.RWWithIOReader(reader))
	return types.NewReaderWriter(allOpts...)
}

// NewCacheWriter creates a types.ReaderWriter configured only for writing to cache
func (c *Cache) NewCacheWriter(dataset, filepath string, fileSize types.Bytes, opts ...types.RWOption) *types.ReaderWriter {
	writer := c.NewFileWriter(dataset, filepath, fileSize)

	allOpts := append(opts, types.RWWithIOWriter(writer))
	return types.NewReaderWriter(allOpts...)
}

// scanDiskFiles scans the disk storage to populate the file index with existing files
func (c *Cache) scanDiskFiles() error {
	return c.fileStorage.ScanFiles(func(dataset, filepath string, fileSize types.Bytes) {
		// Create dataset if it doesn't exist
		if _, exists := c.datasets[dataset]; !exists {
			c.datasets[dataset] = NewDataSet(dataset)
		}

		// Create file index entry for existing file
		fileKey := GetFileKey(dataset, filepath)
		fileIdx := NewFileIndex(dataset, filepath, fileKey, fileSize)
		fileIdx.MarkComplete()
		c.fileIndex[fileKey] = fileIdx

		// Update disk usage tracking
		c.diskUsage += fileSize
	})
}

// restoreFromMetadata restores the cache state from metadata
func (c *Cache) restoreFromMetadata() error {
	metadata := c.metadataManager.GetAllMetadata()

	// If no metadata exists, fall back to scanning disk files
	if len(metadata.FileIndex) == 0 {
		return c.scanDiskFiles()
	}

	// Restore datasets
	for _, datasetMeta := range metadata.Datasets {
		c.datasets[datasetMeta.Name] = NewDataSet(datasetMeta.Name)
	}

	// Restore file index and validate files still exist on disk
	for fileKey, fileMeta := range metadata.FileIndex {
		// Check if file actually exists on disk
		if !c.fileStorage.FileExists(fileMeta.Dataset, fileMeta.FilePath) {
			// File was deleted externally, remove from metadata
			c.metadataManager.RemoveFileMetadata(fileKey)
			continue
		}

		// Create file index entry from metadata
		fileIdx := NewFileIndex(fileMeta.Dataset, fileMeta.FilePath, fileMeta.FileKey, fileMeta.Size)
		if fileMeta.Complete {
			fileIdx.MarkComplete()
		}
		c.fileIndex[fileKey] = fileIdx

		// Add file to dataset
		if dataset, exists := c.datasets[fileMeta.Dataset]; exists {
			// Create a placeholder file entry (actual data will be loaded on demand)
			fileEntry := NewFileEntry(fileMeta.Size)
			dataset.AddFile(fileMeta.FilePath, fileEntry)
		}

		// Update disk usage tracking
		c.diskUsage += fileMeta.Size
	}

	// Save metadata to persist any changes (like removed files)
	return c.metadataManager.SaveMetadata()
}

// GetCacheMetadata returns a copy of all cache metadata
func (c *Cache) GetCacheMetadata() *CacheMetadata {
	return c.metadataManager.GetAllMetadata()
}

// GetFileMetadata returns metadata for a specific file
func (c *Cache) GetFileMetadata(fileKey string) (*FileMetadata, bool) {
	return c.metadataManager.GetFileMetadata(fileKey)
}

// GetDatasetFiles returns all files in a dataset with their metadata
func (c *Cache) GetDatasetFiles(dataset string) map[string]*FileMetadata {
	files := c.metadataManager.GetDatasetFiles(dataset)
	result := make(map[string]*FileMetadata)

	for filepath, fileKey := range files {
		if meta, exists := c.metadataManager.GetFileMetadata(fileKey); exists {
			result[filepath] = meta
		}
	}

	return result
}

// GetAgentFileAvailability returns information about what files this agent has available
func (c *Cache) GetAgentFileAvailability() map[string]map[string]bool {
	metadata := c.metadataManager.GetAllMetadata()
	availability := make(map[string]map[string]bool)

	for _, fileMeta := range metadata.FileIndex {
		if fileMeta.Complete && fileMeta.OnDisk {
			if availability[fileMeta.Dataset] == nil {
				availability[fileMeta.Dataset] = make(map[string]bool)
			}
			availability[fileMeta.Dataset][fileMeta.FilePath] = true
		}
	}

	return availability
}
