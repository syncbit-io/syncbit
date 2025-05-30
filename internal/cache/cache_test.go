package cache

import (
	"context"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"syncbit/internal/config"
	"syncbit/internal/core/types"
)

// Helper function to create a test cache configuration
func createTestCacheConfig(ramLimit types.Bytes) config.CacheConfig {
	return config.CacheConfig{
		RAMLimit:  ramLimit,
		DiskLimit: ramLimit * 2, // Double RAM for disk
	}
}

// Helper function to create a test cache with temporary directory
func createTestCache(ramLimit types.Bytes) (*Cache, func(), error) {
	tmpDir, err := os.MkdirTemp("", "syncbit-cache-test-*")
	if err != nil {
		return nil, nil, err
	}

	cleanup := func() {
		os.RemoveAll(tmpDir)
	}

	cfg := createTestCacheConfig(ramLimit)
	cache, err := NewCache(cfg, tmpDir)
	if err != nil {
		cleanup()
		return nil, nil, err
	}

	return cache, cleanup, nil
}

// Helper function to create test data of specified size
func createTestData(size int) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i % 256)
	}
	return data
}

func TestCacheBasicFileOperations(t *testing.T) {
	ramLimit := types.Bytes(1024 * 1024) // 1MB
	cache, cleanup, err := createTestCache(ramLimit)
	if err != nil {
		t.Fatalf("Failed to create test cache: %v", err)
	}
	defer cleanup()

	// Test storing a file
	testData := createTestData(512) // 512 bytes
	fileKey, err := cache.StoreFile("dataset1", "file1.txt", testData)
	if err != nil {
		t.Fatalf("Failed to store file: %v", err)
	}

	// Test retrieving the file
	retrievedData, err := cache.GetFile(fileKey)
	if err != nil {
		t.Fatalf("Failed to retrieve file: %v", err)
	}

	// Verify data integrity
	if len(retrievedData) != len(testData) {
		t.Fatalf("Data length mismatch: expected %d, got %d", len(testData), len(retrievedData))
	}

	for i := range testData {
		if retrievedData[i] != testData[i] {
			t.Fatalf("Data mismatch at index %d: expected %d, got %d", i, testData[i], retrievedData[i])
		}
	}

	// Test file existence
	if !cache.HasFile(fileKey) {
		t.Fatalf("File should exist in cache")
	}

	if !cache.HasFileByPath("dataset1", "file1.txt") {
		t.Fatalf("File should exist by path")
	}

	// Test cache stats
	stats := cache.GetCacheStats()
	if stats.FilesInRAM != 1 {
		t.Fatalf("Expected 1 file in RAM, got %d", stats.FilesInRAM)
	}
	if stats.TotalFiles != 1 {
		t.Fatalf("Expected 1 total file, got %d", stats.TotalFiles)
	}
}

func TestCacheEviction(t *testing.T) {
	ramLimit := types.Bytes(1024) // 1KB limit
	cache, cleanup, err := createTestCache(ramLimit)
	if err != nil {
		t.Fatalf("Failed to create test cache: %v", err)
	}
	defer cleanup()

	// Store files that exceed RAM limit
	testData1 := createTestData(600) // 600 bytes
	testData2 := createTestData(600) // 600 bytes (total 1200 > 1024)

	fileKey1, err := cache.StoreFile("dataset1", "file1.txt", testData1)
	if err != nil {
		t.Fatalf("Failed to store first file: %v", err)
	}

	fileKey2, err := cache.StoreFile("dataset1", "file2.txt", testData2)
	if err != nil {
		t.Fatalf("Failed to store second file: %v", err)
	}

	// Check that eviction occurred
	stats := cache.GetCacheStats()
	if stats.RAMUsage > ramLimit {
		t.Fatalf("RAM usage %d exceeds limit %d", stats.RAMUsage, ramLimit)
	}

	// Both files should still be accessible (from disk)
	if !cache.HasFile(fileKey1) {
		t.Fatalf("First file should still be accessible")
	}
	if !cache.HasFile(fileKey2) {
		t.Fatalf("Second file should still be accessible")
	}

	// Verify we can still read the files
	data1, err := cache.GetFile(fileKey1)
	if err != nil {
		t.Fatalf("Failed to read first file: %v", err)
	}
	if len(data1) != len(testData1) {
		t.Fatalf("First file data length mismatch")
	}

	data2, err := cache.GetFile(fileKey2)
	if err != nil {
		t.Fatalf("Failed to read second file: %v", err)
	}
	if len(data2) != len(testData2) {
		t.Fatalf("Second file data length mismatch")
	}
}

func TestCacheStreamStore(t *testing.T) {
	ramLimit := types.Bytes(1024 * 1024) // 1MB
	cache, cleanup, err := createTestCache(ramLimit)
	if err != nil {
		t.Fatalf("Failed to create test cache: %v", err)
	}
	defer cleanup()

	// Create test data
	testData := createTestData(2048) // 2KB
	reader := &testReader{data: testData}

	// Store via stream
	err = cache.StreamStore("dataset1", "stream_file.txt", reader, types.Bytes(len(testData)))
	if err != nil {
		t.Fatalf("Failed to stream store: %v", err)
	}

	// Verify file exists and data is correct
	if !cache.HasFileByPath("dataset1", "stream_file.txt") {
		t.Fatalf("Stream stored file should exist")
	}

	retrievedData, err := cache.GetFileByPath("dataset1", "stream_file.txt")
	if err != nil {
		t.Fatalf("Failed to retrieve stream stored file: %v", err)
	}

	if len(retrievedData) != len(testData) {
		t.Fatalf("Stream stored data length mismatch")
	}

	for i := range testData {
		if retrievedData[i] != testData[i] {
			t.Fatalf("Stream stored data mismatch at index %d", i)
		}
	}
}

// testReader implements io.Reader for testing
type testReader struct {
	data []byte
	pos  int
}

func (r *testReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, nil
	}

	n := copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}

func TestCacheReadThroughWriteThrough(t *testing.T) {
	tempDir := t.TempDir()
	cfg := config.CacheConfig{
		RAMLimit:  1024 * 1024,      // 1MB
		DiskLimit: 10 * 1024 * 1024, // 10MB
	}

	cache, err := NewCache(cfg, tempDir)
	require.NoError(t, err)

	dataset := "test-dataset"
	filepath := "test-file.bin"
	testData := []byte("Hello, this is test data for read-through and write-through testing!")
	fileSize := types.Bytes(len(testData))

	// Test 1: Write-through behavior
	t.Run("write-through", func(t *testing.T) {
		// Create a cache writer ReaderWriter
		rw := cache.NewCacheWriter(dataset, filepath, fileSize)

		// Write data through the ReaderWriter
		writer := rw.Writer(context.Background())
		n, err := writer.Write(testData)
		require.NoError(t, err)
		require.Equal(t, len(testData), n)

		// Close to trigger write-through
		err = rw.CloseWriter()
		require.NoError(t, err)

		// Verify file exists in cache
		assert.True(t, cache.HasFileByPath(dataset, filepath))

		// Verify file was written to disk
		diskData, err := cache.fileStorage.ReadFile(dataset, filepath)
		require.NoError(t, err)
		assert.Equal(t, testData, diskData)
	})

	// Test 2: Clear RAM cache to test read-through
	t.Run("clear-ram-for-read-through", func(t *testing.T) {
		// Clear the RAM cache but keep disk files
		cache.fileCache = make(map[string]*FileEntry)
		cache.ramUsage = 0
		cache.lruCache = NewLRUCache(cfg.RAMLimit)

		// File should still be tracked in file index
		assert.True(t, cache.HasFileByPath(dataset, filepath))
	})

	// Test 3: Read-through behavior
	t.Run("read-through", func(t *testing.T) {
		// Create a cache reader ReaderWriter
		rw := cache.NewCacheReader(dataset, filepath, fileSize)

		// Read data through the ReaderWriter
		reader := rw.Reader(context.Background())
		readData := make([]byte, len(testData))
		n, err := reader.Read(readData)
		require.NoError(t, err)
		require.Equal(t, len(testData), n)

		// Verify data is correct
		assert.Equal(t, testData, readData)

		// Verify file was loaded back into RAM cache
		fileKey := GetFileKey(dataset, filepath)
		_, exists := cache.fileCache[fileKey]
		assert.True(t, exists, "File should be loaded back into RAM cache after read-through")
	})

	// Test 4: Full round-trip with rate limiting and progress tracking
	t.Run("full-integration-with-middleware", func(t *testing.T) {
		newFilepath := "integration-test.bin"
		largerData := make([]byte, 1024)
		for i := range largerData {
			largerData[i] = byte(i % 256)
		}

		var progressBytes int64
		progressCallback := func(n int64) {
			progressBytes += n
		}

		// Create rate limiter (generous for testing)
		limiter := types.NewRateLimiter(types.Bytes(1024*1024), types.Bytes(1024*1024), 1) // 1MB/s

		// Test write with progress and rate limiting
		writeRW := cache.NewCacheWriter(dataset, newFilepath, types.Bytes(len(largerData)),
			types.RWWithWriteLimiter(limiter),
			types.RWWithWriterCallback(progressCallback),
		)

		writer := writeRW.Writer(context.Background())
		n, err := writer.Write(largerData)
		require.NoError(t, err)
		require.Equal(t, len(largerData), n)

		err = writeRW.CloseWriter()
		require.NoError(t, err)

		// Verify progress was tracked
		assert.Equal(t, int64(len(largerData)), progressBytes)

		// Clear RAM again
		cache.fileCache = make(map[string]*FileEntry)
		cache.ramUsage = 0
		cache.lruCache = NewLRUCache(cfg.RAMLimit)

		// Test read with rate limiting
		progressBytes = 0 // Reset counter
		readRW := cache.NewCacheReader(dataset, newFilepath, types.Bytes(len(largerData)),
			types.RWWithReadLimiter(limiter),
			types.RWWithReaderCallback(progressCallback),
		)

		reader := readRW.Reader(context.Background())
		readData := make([]byte, len(largerData))
		totalRead, err := io.ReadFull(reader, readData)
		require.NoError(t, err)
		require.Equal(t, len(largerData), totalRead)

		// Verify data integrity
		assert.Equal(t, largerData, readData)

		// Verify progress was tracked
		assert.Equal(t, int64(len(largerData)), progressBytes)
	})
}
