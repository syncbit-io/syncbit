package cache

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"syncbit/internal/core/types"
)

// FileStorage manages actual files on disk at their target locations
type FileStorage interface {
	// File-level operations (new)
	WriteFile(dataset, filePath string, data []byte) error
	ReadFile(dataset, filePath string) ([]byte, error)

	// Block-level operations (legacy, for compatibility)
	WriteBlockAt(dataset, filePath string, blockIndex int, blockSize types.Bytes, data []byte) error
	ReadBlockAt(dataset, filePath string, blockIndex int, blockSize types.Bytes) ([]byte, error)

	// Common operations
	EnsureFile(dataset, filePath string, fileSize types.Bytes) error
	FileExists(dataset, filePath string) bool
	DeleteFile(dataset, filePath string) error
	GetFileSize(dataset, filePath string) (types.Bytes, error)

	// Scanning operations
	ScanFiles(callback func(dataset, filePath string, fileSize types.Bytes)) error
}

// DiskFileStorage implements FileStorage for actual files on disk
type DiskFileStorage struct {
	mu       sync.RWMutex
	basePath string
}

// NewDiskFileStorage creates a new disk-based file storage
func NewDiskFileStorage(basePath string) (*DiskFileStorage, error) {
	if basePath == "" {
		return nil, fmt.Errorf("base path cannot be empty")
	}

	// Create directory if it doesn't exist
	if err := os.MkdirAll(basePath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create storage directory: %w", err)
	}

	return &DiskFileStorage{
		basePath: basePath,
	}, nil
}

// getFilePath returns the full path for a file
func (dfs *DiskFileStorage) getFilePath(dataset, filePath string) string {
	return filepath.Join(dfs.basePath, dataset, filePath)
}

// WriteFile writes complete file data to disk
func (dfs *DiskFileStorage) WriteFile(dataset, filePath string, data []byte) error {
	dfs.mu.Lock()
	defer dfs.mu.Unlock()

	fullPath := dfs.getFilePath(dataset, filePath)

	// Ensure directory exists
	dir := filepath.Dir(fullPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// Write file atomically by writing to temp file and renaming
	tempPath := fullPath + ".tmp"
	if err := ioutil.WriteFile(tempPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}

	if err := os.Rename(tempPath, fullPath); err != nil {
		os.Remove(tempPath) // Clean up temp file on error
		return fmt.Errorf("failed to rename temp file: %w", err)
	}

	return nil
}

// ReadFile reads complete file data from disk
func (dfs *DiskFileStorage) ReadFile(dataset, filePath string) ([]byte, error) {
	dfs.mu.RLock()
	defer dfs.mu.RUnlock()

	fullPath := dfs.getFilePath(dataset, filePath)

	data, err := ioutil.ReadFile(fullPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	return data, nil
}

// EnsureFile creates a file if it doesn't exist and ensures it has the correct size
func (dfs *DiskFileStorage) EnsureFile(dataset, filePath string, fileSize types.Bytes) error {
	fullPath := dfs.getFilePath(dataset, filePath)

	// Create directory if it doesn't exist
	dir := filepath.Dir(fullPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", dir, err)
	}

	// Always create/truncate file to ensure correct size
	file, err := os.Create(fullPath)
	if err != nil {
		return fmt.Errorf("failed to create file %s: %w", fullPath, err)
	}
	defer file.Close()

	// Truncate to the correct size (creates sparse file)
	if err := file.Truncate(int64(fileSize)); err != nil {
		return fmt.Errorf("failed to set file size: %w", err)
	}

	return nil
}

// WriteBlockAt writes a block at the specified block index within a file
func (dfs *DiskFileStorage) WriteBlockAt(dataset, filePath string, blockIndex int, blockSize types.Bytes, data []byte) error {
	fullPath := dfs.getFilePath(dataset, filePath)

	// Calculate offset
	offset := int64(blockIndex) * int64(blockSize)

	// Open file for writing
	file, err := os.OpenFile(fullPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", fullPath, err)
	}
	defer file.Close()

	// Write at offset
	n, err := file.WriteAt(data, offset)
	if err != nil {
		return fmt.Errorf("failed to write at offset %d: %w", offset, err)
	}

	if n != len(data) {
		return fmt.Errorf("incomplete write: wrote %d bytes, expected %d", n, len(data))
	}

	return nil
}

// ReadBlockAt reads a block at the specified block index within a file
func (dfs *DiskFileStorage) ReadBlockAt(dataset, filePath string, blockIndex int, blockSize types.Bytes) ([]byte, error) {
	fullPath := dfs.getFilePath(dataset, filePath)

	// Calculate offset
	offset := int64(blockIndex) * int64(blockSize)

	// Open file for reading
	file, err := os.Open(fullPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", fullPath, err)
	}
	defer file.Close()

	// Get file size to ensure we don't read beyond EOF
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat file: %w", err)
	}

	// Calculate how much to read
	fileSize := fileInfo.Size()
	if offset >= fileSize {
		return nil, fmt.Errorf("offset %d is beyond file size %d", offset, fileSize)
	}

	remainingInFile := fileSize - offset
	bytesToRead := int64(blockSize)
	if bytesToRead > remainingInFile {
		bytesToRead = remainingInFile
	}

	// Read at offset
	data := make([]byte, bytesToRead)
	n, err := file.ReadAt(data, offset)
	if err != nil && err.Error() != "EOF" {
		return nil, fmt.Errorf("failed to read at offset %d: %w", offset, err)
	}

	// Return the actual bytes read (may be less than blockSize for the last block)
	return data[:n], nil
}

// FileExists checks if a file exists
func (dfs *DiskFileStorage) FileExists(dataset, filePath string) bool {
	fullPath := dfs.getFilePath(dataset, filePath)
	_, err := os.Stat(fullPath)
	return !os.IsNotExist(err)
}

// DeleteFile removes a file
func (dfs *DiskFileStorage) DeleteFile(dataset, filePath string) error {
	fullPath := dfs.getFilePath(dataset, filePath)
	if err := os.Remove(fullPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete file %s: %w", fullPath, err)
	}
	return nil
}

// GetFileSize returns the current size of a file
func (dfs *DiskFileStorage) GetFileSize(dataset, filePath string) (types.Bytes, error) {
	fullPath := dfs.getFilePath(dataset, filePath)

	fileInfo, err := os.Stat(fullPath)
	if err != nil {
		return 0, fmt.Errorf("failed to stat file %s: %w", fullPath, err)
	}

	return types.Bytes(fileInfo.Size()), nil
}

// ScanFiles scans all files in the storage and calls the callback for each file
func (dfs *DiskFileStorage) ScanFiles(callback func(dataset, filePath string, fileSize types.Bytes)) error {
	dfs.mu.RLock()
	defer dfs.mu.RUnlock()

	err := filepath.Walk(dfs.basePath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		relPath, err := filepath.Rel(dfs.basePath, path)
		if err != nil {
			return err
		}

		parts := strings.Split(relPath, string(filepath.Separator))
		if len(parts) < 2 {
			return nil
		}

		dataset := parts[0]
		filePath := strings.Join(parts[1:], string(filepath.Separator))

		fileSize, err := dfs.GetFileSize(dataset, filePath)
		if err != nil {
			return err
		}

		callback(dataset, filePath, fileSize)

		return nil
	})

	return err
}
