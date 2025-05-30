package cache

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"

	"syncbit/internal/config"
	"syncbit/internal/core/types"
)

// ExampleProviderIntegration demonstrates how a provider should use the cache
// instead of direct file operations
func ExampleProviderIntegration() {
	// Create cache with typical configuration
	cfg := config.CacheConfig{
		RAMLimit:  types.Bytes(1024 * 1024 * 1024), // 1GB RAM cache
		DiskLimit: types.Bytes(0),                  // Unlimited disk
	}

	cache, err := NewCache(cfg, "/var/lib/syncbit/cache")
	if err != nil {
		log.Fatalf("Failed to create cache: %v", err)
	}

	// Example: Download a model file through the cache
	dataset := "huggingface/llama-2-7b"
	filepath := "pytorch_model.bin"
	fileSize := types.Bytes(5 * 1024 * 1024) // 5MB model file for demo

	// Create a cache-backed ReaderWriter for the file
	rw := cache.NewReaderWriter(dataset, filepath, fileSize)

	// Simulate downloading from a remote source
	ctx := context.Background()

	// In a real provider, you would get data from HTTP, S3, etc.
	// Here we simulate with mock data
	mockFileData := make([]byte, fileSize)
	for i := range mockFileData {
		mockFileData[i] = byte(i % 256)
	}

	// Write complete file through the cache (write-through to disk)
	writer := rw.Writer(ctx)
	n, err := writer.Write(mockFileData)
	if err != nil {
		log.Fatalf("Failed to write file: %v", err)
	}
	fmt.Printf("Wrote complete file: %d bytes\n", n)

	// Close to ensure file is stored
	if err := writer.Close(); err != nil {
		log.Fatalf("Failed to close writer: %v", err)
	}

	// Read data back through the cache (read-through from memory/disk)
	reader := rw.Reader(ctx)

	// Read some data back
	readBuffer := make([]byte, 1024) // 1KB buffer
	totalRead := 0

	for i := 0; i < 3; i++ { // Read first 3KB
		n, err := reader.Read(readBuffer)
		if err != nil && err != io.EOF {
			log.Fatalf("Failed to read data: %v", err)
		}
		totalRead += n
		fmt.Printf("Read iteration %d: %d bytes (total: %d)\n", i, n, totalRead)

		if err == io.EOF {
			break
		}
	}

	// Close reader
	reader.Close()

	// Check cache statistics
	stats := cache.GetCacheStats()
	fmt.Printf("\nCache Statistics:\n")
	fmt.Printf("  RAM Usage: %d bytes (%.1f%% of limit)\n",
		stats.RAMUsage, float64(stats.RAMUsage)/float64(stats.RAMLimit)*100)
	fmt.Printf("  Files in RAM: %d\n", stats.FilesInRAM)
	fmt.Printf("  Total Files: %d\n", stats.TotalFiles)

	if stats.LRUStats != nil {
		fmt.Printf("  Cache Size: %d files\n", stats.LRUStats.CurrentSize)
		fmt.Printf("  Cache Usage: %d bytes\n", stats.LRUStats.CurrentUsage)
	}

	fmt.Println("\nProvider integration example completed successfully!")
}

// ExamplePeerToPeerTransfer demonstrates how agents can share complete files
func ExamplePeerToPeerTransfer() {
	// Create two caches representing different agents
	cfg := config.CacheConfig{
		RAMLimit:  types.Bytes(512 * 1024 * 1024), // 512MB each
		DiskLimit: types.Bytes(0),                 // Unlimited disk
	}

	// Agent 1 cache (has the data)
	agent1Cache, err := NewCache(cfg, "/tmp/agent1")
	if err != nil {
		log.Fatalf("Failed to create agent1 cache: %v", err)
	}

	// Agent 2 cache (needs the data)
	agent2Cache, err := NewCache(cfg, "/tmp/agent2")
	if err != nil {
		log.Fatalf("Failed to create agent2 cache: %v", err)
	}

	// Agent 1 stores a complete file
	dataset := "shared-dataset"
	filepath := "shared-file.bin"
	fileSize := types.Bytes(1024 * 1024) // 1MB file

	testData := make([]byte, fileSize)
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	// Store file in agent 1
	fileKey, err := agent1Cache.StoreFile(dataset, filepath, testData)
	if err != nil {
		log.Fatalf("Failed to store file in agent1: %v", err)
	}
	fmt.Printf("Agent 1 stored file with key: %s\n", fileKey)

	// Simulate peer-to-peer transfer: Agent 2 requests file from Agent 1
	// In reality, this would be an HTTP request to /datasets/{dataset}/files/{filepath}
	fileData, err := agent1Cache.GetFileByPath(dataset, filepath)
	if err != nil {
		log.Fatalf("Agent 1 failed to serve file: %v", err)
	}
	fmt.Printf("Agent 1 served file: %d bytes\n", len(fileData))

	// Agent 2 stores the received file
	receivedKey, err := agent2Cache.StoreFile(dataset, filepath, fileData)
	if err != nil {
		log.Fatalf("Agent 2 failed to store received file: %v", err)
	}
	fmt.Printf("Agent 2 stored received file with key: %s\n", receivedKey)

	// Verify both agents have the file
	if !agent1Cache.HasFileByPath(dataset, filepath) {
		log.Fatalf("Agent 1 doesn't have the file!")
	}
	if !agent2Cache.HasFileByPath(dataset, filepath) {
		log.Fatalf("Agent 2 doesn't have the file!")
	}

	fmt.Println("Peer-to-peer transfer completed successfully!")
	fmt.Println("Both agents now have the complete file")
}

// ExampleMultiFileWorkload demonstrates handling multiple files with different access patterns
func ExampleMultiFileWorkload() {
	cfg := config.CacheConfig{
		RAMLimit:  types.Bytes(256 * 1024 * 1024), // 256MB cache
		DiskLimit: types.Bytes(0),                 // Unlimited disk
	}

	cache, err := NewCache(cfg, "/tmp/multifile")
	if err != nil {
		log.Fatalf("Failed to create cache: %v", err)
	}

	dataset := "ml-workload"

	// Simulate different types of files with different access patterns
	files := []struct {
		name        string
		size        types.Bytes
		accessCount int // How many times we'll access this file
	}{
		{"config.json", types.Bytes(4 * 1024), 20},              // Small, hot file
		{"tokenizer.model", types.Bytes(2 * 1024 * 1024), 5},    // Medium, warm file
		{"pytorch_model.bin", types.Bytes(50 * 1024 * 1024), 2}, // Large, cold file
	}

	// Create and populate files
	for _, file := range files {
		fmt.Printf("Creating file: %s (%d bytes)\n", file.name, file.size)

		// Create test data
		data := make([]byte, file.size)
		for i := range data {
			data[i] = byte(i % 256)
		}

		// Store file through cache
		_, err := cache.StoreFile(dataset, file.name, data)
		if err != nil {
			log.Fatalf("Failed to store %s: %v", file.name, err)
		}
	}

	// Simulate access patterns
	fmt.Println("\nSimulating access patterns...")
	for _, file := range files {
		for i := 0; i < file.accessCount; i++ {
			// Access the file (this will load it into RAM cache if not already there)
			_, err := cache.GetFileByPath(dataset, file.name)
			if err != nil {
				log.Fatalf("Failed to access %s: %v", file.name, err)
			}
		}
		fmt.Printf("Accessed %s %d times\n", file.name, file.accessCount)
	}

	// Show final cache state
	stats := cache.GetCacheStats()
	fmt.Printf("\nFinal Cache State:\n")
	fmt.Printf("  RAM Usage: %d bytes (%.1f%% of limit)\n",
		stats.RAMUsage, float64(stats.RAMUsage)/float64(stats.RAMLimit)*100)
	fmt.Printf("  Files in RAM: %d\n", stats.FilesInRAM)
	fmt.Printf("  Total Files: %d\n", stats.TotalFiles)

	if stats.LRUStats != nil {
		fmt.Printf("  Cache Size: %d files\n", stats.LRUStats.CurrentSize)
		fmt.Printf("  Cache Usage: %d bytes\n", stats.LRUStats.CurrentUsage)
	}

	// The small, frequently accessed config file should still be in RAM
	// while the large model file may have been evicted
	fmt.Println("\nLRU eviction should have kept recently accessed files in memory while evicting older large files")
}

// ExampleDownloadWithCache demonstrates how to download a file using the cache system
// with proper read-through and write-through behavior, rate limiting, and progress tracking
func ExampleDownloadWithCache() {
	// Initialize cache
	cfg := config.CacheConfig{
		RAMLimit:  100 * 1024 * 1024,  // 100MB
		DiskLimit: 1024 * 1024 * 1024, // 1GB
	}
	cache, err := NewCache(cfg, "/tmp/syncbit-cache")
	if err != nil {
		panic(err)
	}

	// Download parameters
	dataset := "huggingface-models"
	filepath := "model.bin"
	downloadURL := "https://example.com/model.bin"
	fileSize := types.Bytes(50 * 1024 * 1024) // 50MB

	// Create HTTP client and request
	resp, err := http.Get(downloadURL)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	// Set up progress tracking
	var downloadedBytes int64
	progressCallback := func(n int64) {
		downloadedBytes += n
		fmt.Printf("Downloaded: %d/%d bytes (%.1f%%)\n",
			downloadedBytes, fileSize, float64(downloadedBytes)/float64(fileSize)*100)
	}

	// Create rate limiter (10MB/s with 1MB burst)
	limiter := types.NewRateLimiter(types.Bytes(10*1024*1024), types.Bytes(1024*1024), 1)

	// Create cache-backed writer with middleware
	rw := cache.NewCacheWriter(dataset, filepath, fileSize,
		types.RWWithIOReader(resp.Body),
		types.RWWithWriteLimiter(limiter),
		types.RWWithReaderCallback(progressCallback),
	)

	// Transfer data from HTTP response to cache (write-through)
	ctx := context.Background()
	bytesTransferred, err := rw.Transfer(ctx)
	if err != nil {
		panic(err)
	}

	// Close writer to ensure data is persisted to disk
	err = rw.CloseWriter()
	if err != nil {
		panic(err)
	}

	fmt.Printf("Successfully downloaded %d bytes to cache\n", bytesTransferred)
}

// ExampleServeFileFromCache demonstrates how to serve a file from cache
// with proper read-through behavior and streaming
func ExampleServeFileFromCache(w http.ResponseWriter, r *http.Request) {
	// Initialize cache (in real usage, this would be a singleton)
	cfg := config.CacheConfig{
		RAMLimit:  100 * 1024 * 1024,  // 100MB
		DiskLimit: 1024 * 1024 * 1024, // 1GB
	}
	cache, err := NewCache(cfg, "/tmp/syncbit-cache")
	if err != nil {
		http.Error(w, "Cache initialization failed", http.StatusInternalServerError)
		return
	}

	// Extract file parameters from request
	dataset := r.URL.Query().Get("dataset")
	filepath := r.URL.Query().Get("file")

	if dataset == "" || filepath == "" {
		http.Error(w, "Dataset and file parameters required", http.StatusBadRequest)
		return
	}

	// Check if file exists (read-through aware)
	if !cache.HasFileByPath(dataset, filepath) {
		http.Error(w, "File not found", http.StatusNotFound)
		return
	}

	// Get file size for proper HTTP headers
	data, err := cache.GetFileByPath(dataset, filepath)
	if err != nil {
		http.Error(w, "Failed to access file", http.StatusInternalServerError)
		return
	}
	fileSize := types.Bytes(len(data))

	// Create rate limiter for upload (5MB/s)
	limiter := types.NewRateLimiter(types.Bytes(5*1024*1024), types.Bytes(1024*1024), 1)

	// Create cache reader with rate limiting
	rw := cache.NewCacheReader(dataset, filepath, fileSize,
		types.RWWithReadLimiter(limiter),
	)

	// Get reader for streaming
	reader := rw.Reader(r.Context())
	defer func() {
		if closer, ok := reader.(io.Closer); ok {
			closer.Close()
		}
	}()

	// Set appropriate HTTP headers
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", fmt.Sprintf("%d", fileSize))
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", filepath))

	// Stream file to client
	_, err = io.Copy(w, reader)
	if err != nil {
		// Log error but don't send HTTP error as headers are already sent
		fmt.Printf("Error streaming file: %v\n", err)
	}
}

// ExampleP2PFileTransfer demonstrates how to use the cache for P2P file transfers
func ExampleP2PFileTransfer() {
	// Initialize cache
	cfg := config.CacheConfig{
		RAMLimit:  100 * 1024 * 1024,  // 100MB
		DiskLimit: 1024 * 1024 * 1024, // 1GB
	}
	cache, err := NewCache(cfg, "/tmp/syncbit-cache")
	if err != nil {
		panic(err)
	}

	dataset := "shared-dataset"
	filepath := "large-file.bin"
	fileSize := types.Bytes(100 * 1024 * 1024) // 100MB

	// Scenario 1: Receiving a file from a peer
	fmt.Println("=== Receiving file from peer ===")

	// Simulate peer data source
	peerData := strings.NewReader("This is simulated data from a peer...")

	// Set up progress tracking for download
	var receivedBytes int64
	receiveCallback := func(n int64) {
		receivedBytes += n
		fmt.Printf("Received from peer: %d bytes\n", receivedBytes)
	}

	// Create cache writer for receiving data
	receiveRW := cache.NewCacheWriter(dataset, filepath, fileSize,
		types.RWWithIOReader(peerData),
		types.RWWithReaderCallback(receiveCallback),
	)

	// Transfer data from peer to cache
	ctx := context.Background()
	_, err = receiveRW.Transfer(ctx)
	if err != nil {
		panic(err)
	}

	err = receiveRW.CloseWriter()
	if err != nil {
		panic(err)
	}

	fmt.Printf("File received and cached successfully\n")

	// Scenario 2: Serving the file to another peer
	fmt.Println("\n=== Serving file to another peer ===")

	// Set up progress tracking for upload
	var sentBytes int64
	sendCallback := func(n int64) {
		sentBytes += n
		fmt.Printf("Sent to peer: %d bytes\n", sentBytes)
	}

	// Create cache reader for serving data
	serveRW := cache.NewCacheReader(dataset, filepath, fileSize,
		types.RWWithReaderCallback(sendCallback),
	)

	// Get reader and simulate sending to peer
	reader := serveRW.Reader(ctx)
	defer func() {
		if closer, ok := reader.(io.Closer); ok {
			closer.Close()
		}
	}()

	// Simulate sending data to peer (in real usage, this would be network I/O)
	buffer := make([]byte, 1024)
	for {
		n, err := reader.Read(buffer)
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}
		// In real usage: send buffer[:n] to peer over network
		_ = buffer[:n]
	}

	fmt.Printf("File served to peer successfully\n")
}

// ExampleCacheWithFallback demonstrates how to implement fallback behavior
// where we try P2P first, then fallback to original source
func ExampleCacheWithFallback() {
	// Initialize cache
	cfg := config.CacheConfig{
		RAMLimit:  100 * 1024 * 1024,  // 100MB
		DiskLimit: 1024 * 1024 * 1024, // 1GB
	}
	cache, err := NewCache(cfg, "/tmp/syncbit-cache")
	if err != nil {
		panic(err)
	}

	dataset := "fallback-test"
	filepath := "test-file.bin"
	fileSize := types.Bytes(10 * 1024 * 1024) // 10MB

	// Check if file is already in cache
	if cache.HasFileByPath(dataset, filepath) {
		fmt.Println("File already in cache, serving from cache")
		return
	}

	// Try P2P download first
	fmt.Println("Attempting P2P download...")
	p2pSuccess := tryP2PDownload(cache, dataset, filepath, fileSize)

	if !p2pSuccess {
		fmt.Println("P2P failed, falling back to original source...")
		err := fallbackToOriginalSource(cache, dataset, filepath, fileSize)
		if err != nil {
			panic(err)
		}
	}

	fmt.Println("File successfully obtained and cached")
}

// tryP2PDownload simulates attempting to download from peers
func tryP2PDownload(cache *Cache, dataset, filepath string, fileSize types.Bytes) bool {
	// In real implementation, this would:
	// 1. Query controller for peer sources
	// 2. Try to connect to peers
	// 3. Download blocks from multiple peers
	// 4. Use cache.NewCacheWriter with peer readers

	// For this example, simulate P2P failure
	return false
}

// fallbackToOriginalSource downloads from the original source
func fallbackToOriginalSource(cache *Cache, dataset, filepath string, fileSize types.Bytes) error {
	// Simulate original source
	originalData := strings.NewReader("This is data from the original source...")

	// Create cache writer for fallback download
	rw := cache.NewCacheWriter(dataset, filepath, fileSize,
		types.RWWithIOReader(originalData),
	)

	// Transfer data
	ctx := context.Background()
	_, err := rw.Transfer(ctx)
	if err != nil {
		return err
	}

	return rw.CloseWriter()
}
