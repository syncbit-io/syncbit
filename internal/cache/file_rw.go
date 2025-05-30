package cache

import (
	"fmt"
	"io"
	"sync"

	"syncbit/internal/core/types"
)

// CacheFileReader provides a read-through interface to cached files
type CacheFileReader struct {
	cache    *Cache
	dataset  string
	filepath string
	fileSize types.Bytes
	offset   int64
	mu       sync.Mutex
}

// CacheFileWriter provides a write-through interface to cached files
type CacheFileWriter struct {
	cache    *Cache
	dataset  string
	filepath string
	fileSize types.Bytes
	data     []byte
	mu       sync.Mutex
}

// NewCacheFileReader creates a new cache-backed file reader
func (c *Cache) NewFileReader(dataset, filepath string, fileSize types.Bytes) *CacheFileReader {
	return &CacheFileReader{
		cache:    c,
		dataset:  dataset,
		filepath: filepath,
		fileSize: fileSize,
		offset:   0,
	}
}

// NewCacheFileWriter creates a new cache-backed file writer
func (c *Cache) NewFileWriter(dataset, filepath string, fileSize types.Bytes) *CacheFileWriter {
	return &CacheFileWriter{
		cache:    c,
		dataset:  dataset,
		filepath: filepath,
		fileSize: fileSize,
		data:     make([]byte, fileSize),
	}
}

// Read implements io.Reader interface with read-through caching
func (r *CacheFileReader) Read(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.offset >= int64(r.fileSize) {
		return 0, io.EOF
	}

	// Try to get the complete file from cache
	fileData, err := r.cache.GetFileByPath(r.dataset, r.filepath)
	if err != nil {
		return 0, fmt.Errorf("failed to read file from cache: %w", err)
	}

	// Calculate how much to read
	bytesToRead := len(p)
	remaining := int64(len(fileData)) - r.offset
	if int64(bytesToRead) > remaining {
		bytesToRead = int(remaining)
	}

	// Copy data from the file to the buffer
	copy(p[:bytesToRead], fileData[r.offset:r.offset+int64(bytesToRead)])
	r.offset += int64(bytesToRead)

	return bytesToRead, nil
}

// WriteAt implements io.WriterAt interface with write-through caching
func (w *CacheFileWriter) WriteAt(p []byte, off int64) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if off < 0 {
		return 0, fmt.Errorf("negative offset: %d", off)
	}

	// Ensure we have enough space in our buffer
	endPos := off + int64(len(p))
	if endPos > int64(len(w.data)) {
		// Extend buffer if needed
		newData := make([]byte, endPos)
		copy(newData, w.data)
		w.data = newData
		w.fileSize = types.Bytes(endPos)
	}

	// Copy data into our buffer
	copy(w.data[off:], p)

	return len(p), nil
}

// Write implements io.Writer interface
func (w *CacheFileWriter) Write(p []byte) (int, error) {
	return w.WriteAt(p, int64(len(w.data)))
}

// Close implements io.Closer interface - stores the complete file
func (w *CacheFileWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Store the complete file in cache
	_, err := w.cache.StoreFile(w.dataset, w.filepath, w.data)
	return err
}

// Close implements io.Closer interface (no-op for reader)
func (r *CacheFileReader) Close() error {
	return nil
}

// Seek implements io.Seeker interface for the reader
func (r *CacheFileReader) Seek(offset int64, whence int) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	var newOffset int64
	switch whence {
	case io.SeekStart:
		newOffset = offset
	case io.SeekCurrent:
		newOffset = r.offset + offset
	case io.SeekEnd:
		newOffset = int64(r.fileSize) + offset
	default:
		return 0, fmt.Errorf("invalid whence value: %d", whence)
	}

	if newOffset < 0 {
		return 0, fmt.Errorf("negative offset: %d", newOffset)
	}

	r.offset = newOffset
	return newOffset, nil
}

// ReaderWriter provides unified access to cached files
type ReaderWriter struct {
	cache    *Cache
	dataset  string
	filepath string
	fileSize types.Bytes
}

// NewReaderWriter creates a new cache-backed file reader/writer
func (c *Cache) NewReaderWriter(dataset, filepath string, fileSize types.Bytes) *ReaderWriter {
	return &ReaderWriter{
		cache:    c,
		dataset:  dataset,
		filepath: filepath,
		fileSize: fileSize,
	}
}

// Reader returns a reader for the file
func (rw *ReaderWriter) Reader(ctx interface{}) io.ReadCloser {
	return rw.cache.NewFileReader(rw.dataset, rw.filepath, rw.fileSize)
}

// Writer returns a writer for the file
func (rw *ReaderWriter) Writer(ctx interface{}) io.WriteCloser {
	return rw.cache.NewFileWriter(rw.dataset, rw.filepath, rw.fileSize)
}

// WriterAt returns a writer that supports WriteAt for the file
func (rw *ReaderWriter) WriterAt(ctx interface{}) io.WriterAt {
	return rw.cache.NewFileWriter(rw.dataset, rw.filepath, rw.fileSize)
}
