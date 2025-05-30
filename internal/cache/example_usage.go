package cache

import (
	"context"
	"fmt"
	"io"
	"log"

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
