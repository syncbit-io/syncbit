package types

import "time"

// FileInfo represents comprehensive file metadata for the cache system
type FileInfo struct {
	// Core file identification
	Path    string `json:"path"`     // File path within dataset
	Size    Bytes  `json:"size"`     // File size in bytes
	ModTime time.Time `json:"mod_time"` // Last modification time
	ETag    string `json:"etag"`     // Entity tag for validation
	
	// Cache context (optional, for cache operations)
	Dataset string `json:"dataset,omitempty"` // Dataset name (empty for standalone use)
	FileKey string `json:"file_key,omitempty"` // Generated cache key (empty for standalone use)
}

// NewFileInfo creates basic file info without cache context
func NewFileInfo(path string, size Bytes, modTime time.Time, eTag string) *FileInfo {
	return &FileInfo{
		Path:    path,
		Size:    size,
		ModTime: modTime,
		ETag:    eTag,
	}
}

// NewCacheFileInfo creates file info with cache context
func NewCacheFileInfo(dataset, path string, size Bytes, modTime time.Time, eTag string) *FileInfo {
	fileKey := dataset + "/" + path // Simple file key generation
	return &FileInfo{
		Path:    path,
		Size:    size,
		ModTime: modTime,
		ETag:    eTag,
		Dataset: dataset,
		FileKey: fileKey,
	}
}

// GetFileKey returns the cache key for this file
func (fi *FileInfo) GetFileKey() string {
	if fi.FileKey != "" {
		return fi.FileKey
	}
	if fi.Dataset != "" {
		return fi.Dataset + "/" + fi.Path
	}
	return fi.Path
}
