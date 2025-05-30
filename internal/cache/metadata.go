package cache

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"syncbit/internal/core/types"
)

// CacheMetadata represents the persistent metadata for the cache
type CacheMetadata struct {
	Version   string                   `json:"version"`
	Datasets  map[string]*DatasetMeta  `json:"datasets"`
	FileIndex map[string]*FileMetadata `json:"file_index"`
	UpdatedAt time.Time                `json:"updated_at"`
}

// DatasetMeta contains metadata about a dataset
type DatasetMeta struct {
	Name      string            `json:"name"`
	Files     map[string]string `json:"files"` // filepath -> file_key mapping
	CreatedAt time.Time         `json:"created_at"`
	UpdatedAt time.Time         `json:"updated_at"`
}

// FileMetadata extends FileIndex with additional metadata for persistence
type FileMetadata struct {
	Dataset   string      `json:"dataset"`   // Dataset name
	FilePath  string      `json:"file_path"` // File path within dataset
	FileKey   string      `json:"file_key"`  // File key
	Size      types.Bytes `json:"size"`      // File size
	ModTime   time.Time   `json:"mod_time"`  // File modification time
	ETag      string      `json:"etag"`      // Entity tag for change detection
	Timestamp time.Time   `json:"timestamp"` // When metadata was created/updated
	Complete  bool        `json:"complete"`  // Whether file is fully available
	OnDisk    bool        `json:"on_disk"`   // Whether file exists on disk
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
			Version:   "1.0",
			Datasets:  make(map[string]*DatasetMeta),
			FileIndex: make(map[string]*FileMetadata),
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
func (mm *MetadataManager) AddFileMetadata(fileKey string, meta *FileMetadata) {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	mm.metadata.FileIndex[fileKey] = meta

	// Ensure dataset exists
	if _, exists := mm.metadata.Datasets[meta.Dataset]; !exists {
		mm.metadata.Datasets[meta.Dataset] = &DatasetMeta{
			Name:      meta.Dataset,
			Files:     make(map[string]string),
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		}
	}

	// Add file to dataset
	mm.metadata.Datasets[meta.Dataset].Files[meta.FilePath] = fileKey
	mm.metadata.Datasets[meta.Dataset].UpdatedAt = time.Now()
}

// GetFileMetadata retrieves metadata for a file
func (mm *MetadataManager) GetFileMetadata(fileKey string) (*FileMetadata, bool) {
	mm.mu.RLock()
	defer mm.mu.RUnlock()

	meta, exists := mm.metadata.FileIndex[fileKey]
	return meta, exists
}

// GetDatasetFiles returns all files in a dataset
func (mm *MetadataManager) GetDatasetFiles(dataset string) map[string]string {
	mm.mu.RLock()
	defer mm.mu.RUnlock()

	if datasetMeta, exists := mm.metadata.Datasets[dataset]; exists {
		// Return a copy to avoid race conditions
		files := make(map[string]string)
		for path, key := range datasetMeta.Files {
			files[path] = key
		}
		return files
	}

	return make(map[string]string)
}

// RemoveFileMetadata removes metadata for a file
func (mm *MetadataManager) RemoveFileMetadata(fileKey string) {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	if meta, exists := mm.metadata.FileIndex[fileKey]; exists {
		// Remove from dataset
		if datasetMeta, exists := mm.metadata.Datasets[meta.Dataset]; exists {
			delete(datasetMeta.Files, meta.FilePath)
			datasetMeta.UpdatedAt = time.Now()
		}

		// Remove from file index
		delete(mm.metadata.FileIndex, fileKey)
	}
}

// GetAllMetadata returns a copy of all metadata for external use
func (mm *MetadataManager) GetAllMetadata() *CacheMetadata {
	mm.mu.RLock()
	defer mm.mu.RUnlock()

	// Return a deep copy to avoid external modifications
	datasets := make(map[string]*DatasetMeta)
	for name, dataset := range mm.metadata.Datasets {
		files := make(map[string]string)
		for path, key := range dataset.Files {
			files[path] = key
		}
		datasets[name] = &DatasetMeta{
			Name:      dataset.Name,
			Files:     files,
			CreatedAt: dataset.CreatedAt,
			UpdatedAt: dataset.UpdatedAt,
		}
	}

	fileIndex := make(map[string]*FileMetadata)
	for key, meta := range mm.metadata.FileIndex {
		fileIndex[key] = &FileMetadata{
			Dataset:   meta.Dataset,
			FilePath:  meta.FilePath,
			FileKey:   meta.FileKey,
			Size:      meta.Size,
			ModTime:   meta.ModTime,
			ETag:      meta.ETag,
			Timestamp: meta.Timestamp,
			Complete:  meta.Complete,
			OnDisk:    meta.OnDisk,
		}
	}

	return &CacheMetadata{
		Version:   mm.metadata.Version,
		Datasets:  datasets,
		FileIndex: fileIndex,
		UpdatedAt: mm.metadata.UpdatedAt,
	}
}
