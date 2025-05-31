package cache

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"syncbit/internal/core/types"
)

// FileEntry represents a cached file in memory
type FileEntry struct {
	mu        sync.RWMutex
	Size      types.Bytes
	Timestamp time.Time
	Data      []byte
}

// NewFileEntry creates a file entry with the specified size
func NewFileEntry(fileSize types.Bytes) *FileEntry {
	return &FileEntry{
		Size:      fileSize,
		Timestamp: time.Now(),
		Data:      make([]byte, fileSize),
	}
}

// NewFileEntryWithData creates a file entry with existing data
func NewFileEntryWithData(data []byte) *FileEntry {
	fileData := make([]byte, len(data))
	copy(fileData, data)

	return &FileEntry{
		Size:      types.Bytes(len(data)),
		Timestamp: time.Now(),
		Data:      fileData,
	}
}

// Length returns the size of the file
func (f *FileEntry) Length() types.Bytes {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return types.Bytes(len(f.Data))
}

// Write overwrites the file data
func (f *FileEntry) Write(data []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.Data = make([]byte, len(data))
	copy(f.Data, data)
	f.Size = types.Bytes(len(data))
	f.Timestamp = time.Now()
	return nil
}

// Read returns a copy of the file data
func (f *FileEntry) Read() ([]byte, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	result := make([]byte, len(f.Data))
	copy(result, f.Data)
	return result, nil
}

// CacheMetadata represents the persistent metadata for the cache
type CacheMetadata struct {
	Version   string                    `json:"version"`
	FileIndex map[string]*types.FileInfo `json:"file_index"`
	UpdatedAt time.Time                 `json:"updated_at"`
}

// MetadataManager handles loading and saving cache metadata
type MetadataManager struct {
	mu           sync.RWMutex
	metadataPath string
	metadata     *CacheMetadata
}

// NewMetadataManager creates a new metadata manager
func NewMetadataManager(basePath string) *MetadataManager {
	metadataPath := filepath.Join(basePath, "cache_metadata.json")

	return &MetadataManager{
		metadataPath: metadataPath,
		metadata: &CacheMetadata{
			Version:   "0.1.0",
			FileIndex: make(map[string]*types.FileInfo),
			UpdatedAt: time.Now(),
		},
	}
}

// LoadMetadata loads cache metadata from disk if it exists
func (mm *MetadataManager) LoadMetadata() error {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	// Check if metadata file exists
	if _, err := os.Stat(mm.metadataPath); os.IsNotExist(err) {
		// No existing metadata, start fresh
		return nil
	}

	// Read metadata file
	data, err := os.ReadFile(mm.metadataPath)
	if err != nil {
		return fmt.Errorf("failed to read metadata file: %w", err)
	}

	// Parse metadata
	var metadata CacheMetadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return fmt.Errorf("failed to parse metadata: %w", err)
	}

	mm.metadata = &metadata
	return nil
}

// SaveMetadata saves the current cache metadata to disk
func (mm *MetadataManager) SaveMetadata() error {
	mm.mu.RLock()
	defer mm.mu.RUnlock()

	// Update timestamp
	mm.metadata.UpdatedAt = time.Now()

	// Ensure directory exists
	if err := os.MkdirAll(filepath.Dir(mm.metadataPath), 0755); err != nil {
		return fmt.Errorf("failed to create metadata directory: %w", err)
	}

	// Serialize metadata
	data, err := json.MarshalIndent(mm.metadata, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to serialize metadata: %w", err)
	}

	// Write to temporary file first
	tmpPath := mm.metadataPath + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write temporary metadata file: %w", err)
	}

	// Atomic rename
	if err := os.Rename(tmpPath, mm.metadataPath); err != nil {
		os.Remove(tmpPath) // Clean up on error
		return fmt.Errorf("failed to update metadata file: %w", err)
	}

	return nil
}

// AddFileMetadata adds or updates metadata for a file
func (mm *MetadataManager) AddFileMetadata(fileKey string, fileInfo *types.FileInfo) {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	mm.metadata.FileIndex[fileKey] = fileInfo
}

// GetFileMetadata retrieves metadata for a file
func (mm *MetadataManager) GetFileMetadata(fileKey string) (*types.FileInfo, bool) {
	mm.mu.RLock()
	defer mm.mu.RUnlock()

	fileInfo, exists := mm.metadata.FileIndex[fileKey]
	return fileInfo, exists
}

// GetDatasetFiles returns all files in a dataset
func (mm *MetadataManager) GetDatasetFiles(dataset string) map[string]string {
	mm.mu.RLock()
	defer mm.mu.RUnlock()

	files := make(map[string]string)
	for fileKey, fileInfo := range mm.metadata.FileIndex {
		if fileInfo.Dataset == dataset {
			files[fileInfo.Path] = fileKey
		}
	}
	return files
}

// RemoveFileMetadata removes metadata for a file
func (mm *MetadataManager) RemoveFileMetadata(fileKey string) {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	delete(mm.metadata.FileIndex, fileKey)
}

// GetAllMetadata returns a copy of all metadata for external use
func (mm *MetadataManager) GetAllMetadata() *CacheMetadata {
	mm.mu.RLock()
	defer mm.mu.RUnlock()

	// Return a deep copy to avoid external modifications
	fileIndex := make(map[string]*types.FileInfo)
	for key, fileInfo := range mm.metadata.FileIndex {
		// Create a copy of the FileInfo
		fileIndex[key] = &types.FileInfo{
			Path:    fileInfo.Path,
			Size:    fileInfo.Size,
			ModTime: fileInfo.ModTime,
			ETag:    fileInfo.ETag,
			Dataset: fileInfo.Dataset,
			FileKey: fileInfo.FileKey,
		}
	}

	return &CacheMetadata{
		Version:   mm.metadata.Version,
		FileIndex: fileIndex,
		UpdatedAt: mm.metadata.UpdatedAt,
	}
}

// DataSet represents a collection of files in the cache
type DataSet struct {
	name  string
	mu    sync.RWMutex
	files map[string]*FileEntry // file path -> file entry
}

// NewDataSet creates a new dataset
func NewDataSet(name string) *DataSet {
	return &DataSet{
		name:  name,
		files: make(map[string]*FileEntry),
	}
}

// Name returns the dataset name
func (d *DataSet) Name() string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.name
}

// AddFile adds a file to the dataset
func (d *DataSet) AddFile(path string, entry *FileEntry) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.files[path] = entry
}

// GetFile retrieves a file from the dataset
func (d *DataSet) GetFile(path string) (*FileEntry, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	entry, ok := d.files[path]
	if !ok {
		return nil, fmt.Errorf("file not found: %s", path)
	}
	return entry, nil
}

// Cache provides a file-level cache with LRU eviction for efficient P2P file synchronization
type Cache struct {
	mu  sync.RWMutex
	cfg types.CacheConfig

	// Dataset and file organization
	datasets map[string]*DataSet // dataset name -> dataset

	// File cache for in-memory acceleration (keyed by file key)
	fileCache map[string]*FileEntry    // file key -> file entry in memory
	fileIndex map[string]*types.FileInfo // file key -> file metadata

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

	// Cleanup tracking for proper test cleanup
	pendingWrites sync.WaitGroup
	closing       bool
}

// CacheOption allows customizing cache behavior
type CacheOption func(*Cache)

// WithFileStorage sets a custom file storage implementation
func WithFileStorage(storage FileStorage) CacheOption {
	return func(c *Cache) {
		c.fileStorage = storage
	}
}

// NewCache creates a new cache instance with the specified configuration
func NewCache(cfg types.CacheConfig, basePath string, opts ...CacheOption) (*Cache, error) {
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
		fileIndex:       make(map[string]*types.FileInfo),
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

	// Apply options
	for _, opt := range opts {
		opt(cache)
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
	fileInfo := types.NewCacheFileInfo(dataset, filepath, fileSize, time.Now(), "")
	c.fileIndex[fileKey] = fileInfo

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
	fileMeta := types.NewCacheFileInfo(dataset, filepath, fileSize, time.Now(), "")
	c.metadataManager.AddFileMetadata(fileKey, fileMeta)

	// Save metadata to disk (async to avoid blocking)
	c.pendingWrites.Add(1)
	go func() {
		defer c.pendingWrites.Done()
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
	fileInfo, exists := func() (*types.FileInfo, bool) {
		c.mu.RLock()
		defer c.mu.RUnlock()
		info, ok := c.fileIndex[fileKey]
		return info, ok
	}()

	if exists {
		data, err := c.fileStorage.ReadFile(fileInfo.Dataset, fileInfo.Path)
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

	// Check if we have metadata for this file (presence indicates completeness)
	if fileInfo, exists := c.fileIndex[fileKey]; exists {
		// Verify it exists on disk
		return c.fileStorage.FileExists(fileInfo.Dataset, fileInfo.Path)
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

	// Get real disk usage from file storage
	realDiskUsage := c.diskUsage
	if diskUsage, err := c.fileStorage.GetTotalDiskUsage(); err == nil {
		realDiskUsage = diskUsage
	}

	stats := CacheStats{
		RAMUsage:        c.ramUsage,
		RAMLimit:        c.cfg.RAMLimit,
		DiskUsage:       realDiskUsage,
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

// UpdateDiskUsage updates the tracked disk usage by querying the actual disk usage
func (c *Cache) UpdateDiskUsage() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	diskUsage, err := c.fileStorage.GetTotalDiskUsage()
	if err != nil {
		return fmt.Errorf("failed to get disk usage: %w", err)
	}

	c.diskUsage = diskUsage
	return nil
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
		fileInfo := types.NewCacheFileInfo(dataset, filepath, fileSize, time.Now(), "")
		c.fileIndex[fileKey] = fileInfo

		// Update disk usage tracking
		c.diskUsage += fileSize

		// Periodically update real disk usage (every 100 files or so)
		if len(c.fileIndex) % 100 == 0 {
			if realUsage, err := c.fileStorage.GetTotalDiskUsage(); err == nil {
				c.diskUsage = realUsage
			}
		}
	})
}

// restoreFromMetadata restores the cache state from metadata
func (c *Cache) restoreFromMetadata() error {
	metadata := c.metadataManager.GetAllMetadata()

	// If no metadata exists, fall back to scanning disk files
	if len(metadata.FileIndex) == 0 {
		return c.scanDiskFiles()
	}

	// Restore datasets by discovering them from FileIndex
	datasetNames := make(map[string]bool)
	for _, fileInfo := range metadata.FileIndex {
		if fileInfo.Dataset != "" {
			datasetNames[fileInfo.Dataset] = true
		}
	}
	for datasetName := range datasetNames {
		c.datasets[datasetName] = NewDataSet(datasetName)
	}

	// Restore file index and validate files still exist on disk
	for fileKey, fileInfo := range metadata.FileIndex {
		// Check if file actually exists on disk
		if !c.fileStorage.FileExists(fileInfo.Dataset, fileInfo.Path) {
			// File was deleted externally, remove from metadata
			c.metadataManager.RemoveFileMetadata(fileKey)
			continue
		}

		// Create file index entry from metadata (presence indicates completeness)
		c.fileIndex[fileKey] = fileInfo

		// Add file to dataset
		if dataset, exists := c.datasets[fileInfo.Dataset]; exists {
			// Create a placeholder file entry (actual data will be loaded on demand)
			fileEntry := NewFileEntry(fileInfo.Size)
			dataset.AddFile(fileInfo.Path, fileEntry)
		}

		// Update disk usage tracking
		c.diskUsage += fileInfo.Size
	}

	// Get real disk usage after restoring metadata
	if realUsage, err := c.fileStorage.GetTotalDiskUsage(); err == nil {
		c.diskUsage = realUsage
	}

	// Save metadata to persist any changes (like removed files)
	return c.metadataManager.SaveMetadata()
}

// GetCacheMetadata returns a copy of all cache metadata
func (c *Cache) GetCacheMetadata() *CacheMetadata {
	return c.metadataManager.GetAllMetadata()
}

// GetFileMetadata returns metadata for a specific file
func (c *Cache) GetFileMetadata(fileKey string) (*types.FileInfo, bool) {
	return c.metadataManager.GetFileMetadata(fileKey)
}

// GetDatasetFiles returns all files in a dataset with their metadata
func (c *Cache) GetDatasetFiles(dataset string) map[string]*types.FileInfo {
	files := c.metadataManager.GetDatasetFiles(dataset)
	result := make(map[string]*types.FileInfo)

	for filepath, fileKey := range files {
		if fileInfo, exists := c.metadataManager.GetFileMetadata(fileKey); exists {
			result[filepath] = fileInfo
		}
	}

	return result
}

// GetAgentFileAvailability returns information about what files this agent has available
func (c *Cache) GetAgentFileAvailability() map[string]map[string]bool {
	metadata := c.metadataManager.GetAllMetadata()
	availability := make(map[string]map[string]bool)

	for _, fileInfo := range metadata.FileIndex {
		// If it's in the metadata, it's available (presence indicates completeness)
		if availability[fileInfo.Dataset] == nil {
			availability[fileInfo.Dataset] = make(map[string]bool)
		}
		availability[fileInfo.Dataset][fileInfo.Path] = true
	}

	return availability
}

// UpdateFileMetadata updates the metadata for an existing file with source information
func (c *Cache) UpdateFileMetadata(dataset, filepath string, sourceInfo *types.FileInfo) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Find the file in our dataset
	datasetInfo, exists := c.datasets[dataset]
	if !exists {
		return fmt.Errorf("dataset %s not found", dataset)
	}

	_, exists = datasetInfo.files[filepath]
	if !exists {
		return fmt.Errorf("file %s not found in dataset %s", filepath, dataset)
	}

	fileKey := GetFileKey(dataset, filepath)

	// Update the metadata if we have source information
	if sourceInfo != nil {
		fileInfo, exists := c.metadataManager.GetFileMetadata(fileKey)
		if exists && fileInfo != nil {
			if !sourceInfo.ModTime.IsZero() {
				fileInfo.ModTime = sourceInfo.ModTime
			}
			if sourceInfo.ETag != "" {
				fileInfo.ETag = sourceInfo.ETag
			}
			if sourceInfo.Size > 0 {
				fileInfo.Size = sourceInfo.Size
			}

			// Update the metadata in memory and schedule disk save
			c.metadataManager.AddFileMetadata(fileKey, fileInfo)
			
			// Save metadata to disk (async to avoid blocking)
			c.pendingWrites.Add(1)
			go func() {
				defer c.pendingWrites.Done()
				if err := c.metadataManager.SaveMetadata(); err != nil {
					// Log error but don't fail the operation
					// TODO: Add proper logging when available
				}
			}()
		}
	}

	return nil
}

// Close ensures proper cleanup of any background operations
func (c *Cache) Close() error {
	c.mu.Lock()
	c.closing = true
	c.mu.Unlock()

	// Wait for all pending background operations to complete
	c.pendingWrites.Wait()

	return nil
}
