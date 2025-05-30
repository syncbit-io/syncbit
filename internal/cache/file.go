package cache

import (
	"fmt"
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

// FileIndex represents metadata about a file's location and content
type FileIndex struct {
	Dataset   string      `json:"dataset"`   // Dataset name
	FilePath  string      `json:"file_path"` // File path within dataset
	FileKey   string      `json:"file_key"`  // File key
	Size      types.Bytes `json:"size"`      // File size
	Timestamp time.Time   `json:"timestamp"` // When file was created/updated
	Complete  bool        `json:"complete"`  // Whether file is fully available
}

// NewFileIndex creates a new file index entry
func NewFileIndex(dataset, filepath, fileKey string, size types.Bytes) *FileIndex {
	return &FileIndex{
		Dataset:   dataset,
		FilePath:  filepath,
		FileKey:   fileKey,
		Size:      size,
		Timestamp: time.Now(),
		Complete:  false,
	}
}

// GetKey returns the storage key for this file
func (fi *FileIndex) GetKey() string {
	return fi.FileKey
}

// GetFileKey returns the file identifier
func (fi *FileIndex) GetFileKey() string {
	return fmt.Sprintf("%s/%s", fi.Dataset, fi.FilePath)
}

// MarkComplete marks this file as fully available
func (fi *FileIndex) MarkComplete() {
	fi.Complete = true
	fi.Timestamp = time.Now()
}
