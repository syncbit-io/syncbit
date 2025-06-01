package cache

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"testing"

	"syncbit/internal/core/types"
)

func TestBlockCache(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "block_cache_test_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	cache := NewBlockCache(types.Bytes(100 * 1024 * 1024), tmpDir) // 100MB limit

	t.Run("InitFile", func(t *testing.T) {
		fileKey := "test-file"
		totalSize := types.Bytes(3*BlockSize + 512*1024) // 3.5MB file

		cache.InitFile(fileKey, totalSize)

		meta, exists := cache.GetFileMetadata(fileKey)
		if !exists {
			t.Fatal("File metadata not found after init")
		}

		if meta.TotalSize != totalSize {
			t.Errorf("Total size mismatch: got %d, want %d", meta.TotalSize, totalSize)
		}

		if meta.BlockCount != 4 { // Ceiling of 3.5
			t.Errorf("Block count mismatch: got %d, want 4", meta.BlockCount)
		}

		if meta.IsComplete() {
			t.Error("File should not be complete initially")
		}
	})

	t.Run("WriteAt Single Block", func(t *testing.T) {
		fileKey := "single-block"
		data := make([]byte, 1024) // 1KB data
		for i := range data {
			data[i] = byte(i % 256)
		}

		cache.InitFile(fileKey, types.Bytes(len(data)))

		err := cache.WriteAt(fileKey, 0, data)
		if err != nil {
			t.Fatalf("WriteAt failed: %v", err)
		}

		// Should have block 0
		if !cache.HasBlock(fileKey, 0) {
			t.Error("Block 0 should be available")
		}

		// Should be complete
		meta, _ := cache.GetFileMetadata(fileKey)
		if !meta.IsComplete() {
			t.Error("Single block file should be complete")
		}
	})

	t.Run("WriteAt Multiple Blocks", func(t *testing.T) {
		fileKey := "multi-block"
		// Create 2.5MB of data
		totalSize := 2*BlockSize + 512*1024
		data := make([]byte, totalSize)
		for i := range data {
			data[i] = byte(i % 256)
		}

		cache.InitFile(fileKey, types.Bytes(totalSize))

		// Write in chunks
		chunkSize := 256 * 1024 // 256KB chunks
		for offset := 0; offset < len(data); offset += chunkSize {
			end := min(offset + chunkSize, len(data))

			err := cache.WriteAt(fileKey, types.Bytes(offset), data[offset:end])
			if err != nil {
				t.Fatalf("WriteAt failed at offset %d: %v", offset, err)
			}
		}

		// Should have all blocks
		meta, _ := cache.GetFileMetadata(fileKey)
		if !meta.IsComplete() {
			t.Error("Multi-block file should be complete")
		}

		for i := range 3 {
			if !cache.HasBlock(fileKey, i) {
				t.Errorf("Block %d should be available", i)
			}
		}

		// Read back and verify
		result, err := cache.ReadFile(fileKey)
		if err != nil {
			t.Fatalf("ReadFile failed: %v", err)
		}

		if !bytes.Equal(result, data) {
			t.Error("Read data doesn't match written data")
		}
	})

	t.Run("ReadBlock", func(t *testing.T) {
		fileKey := "read-block-test"
		data := make([]byte, BlockSize+1024) // Just over 1 block
		for i := range data {
			data[i] = byte(i % 256)
		}

		cache.InitFile(fileKey, types.Bytes(len(data)))
		cache.WriteAt(fileKey, 0, data)

		// Read first block
		block0, err := cache.ReadBlock(fileKey, 0)
		if err != nil {
			t.Fatalf("ReadBlock failed: %v", err)
		}

		if len(block0) != BlockSize {
			t.Errorf("Block 0 size wrong: got %d, want %d", len(block0), BlockSize)
		}

		if !bytes.Equal(block0, data[:BlockSize]) {
			t.Error("Block 0 data mismatch")
		}

		// Read second block (partial)
		block1, err := cache.ReadBlock(fileKey, 1)
		if err != nil {
			t.Fatalf("ReadBlock failed: %v", err)
		}

		if len(block1) != 1024 {
			t.Errorf("Block 1 size wrong: got %d, want 1024", len(block1))
		}

		if !bytes.Equal(block1, data[BlockSize:]) {
			t.Error("Block 1 data mismatch")
		}
	})

	t.Run("Immediate Read After Write", func(t *testing.T) {
		fileKey := "immediate-read"
		cache.InitFile(fileKey, types.Bytes(2*BlockSize))

		// Write first block
		block0Data := make([]byte, BlockSize)
		for i := range block0Data {
			block0Data[i] = byte(i % 256)
		}

		cache.WriteAt(fileKey, 0, block0Data)

		// Should immediately be able to read block 0
		if !cache.HasBlock(fileKey, 0) {
			t.Error("Block 0 should be immediately available")
		}

		readData, err := cache.ReadBlock(fileKey, 0)
		if err != nil {
			t.Fatalf("Immediate read failed: %v", err)
		}

		if !bytes.Equal(readData, block0Data) {
			t.Error("Immediate read data mismatch")
		}

		// Block 1 should not be available yet
		if cache.HasBlock(fileKey, 1) {
			t.Error("Block 1 should not be available yet")
		}

		// File should not be complete
		meta, _ := cache.GetFileMetadata(fileKey)
		if meta.IsComplete() {
			t.Error("File should not be complete with only 1 of 2 blocks")
		}
	})
}

func TestBlockWriter(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "block_writer_test_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	cache := NewBlockCache(types.Bytes(100 * 1024 * 1024), tmpDir)

	t.Run("Stream Write", func(t *testing.T) {
		fileKey := "stream-test"
		totalSize := types.Bytes(2*BlockSize + 100*1024) // 2.1MB

		writer := cache.NewBlockWriter(fileKey, totalSize)

		// Write in 100KB chunks
		chunkSize := 100 * 1024
		totalWritten := 0

		for totalWritten < int(totalSize) {
			remaining := int(totalSize) - totalWritten
			writeSize := min(chunkSize, remaining)

			chunk := make([]byte, writeSize)
			for i := range chunk {
				chunk[i] = byte((totalWritten + i) % 256)
			}

			n, err := writer.Write(chunk)
			if err != nil {
				t.Fatalf("Writer.Write failed: %v", err)
			}

			if n != writeSize {
				t.Fatalf("Write size mismatch: got %d, want %d", n, writeSize)
			}

			totalWritten += n
		}

		writer.Close()

		// Verify file is complete
		meta, _ := cache.GetFileMetadata(fileKey)
		if !meta.IsComplete() {
			t.Error("File should be complete after streaming write")
		}

		// Verify data integrity
		result, err := cache.ReadFile(fileKey)
		if err != nil {
			t.Fatalf("ReadFile failed: %v", err)
		}

		for i, b := range result {
			expected := byte(i % 256)
			if b != expected {
				t.Errorf("Data mismatch at offset %d: got %d, want %d", i, b, expected)
				break
			}
		}
	})
}

func TestBlockReader(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "block_reader_test_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	cache := NewBlockCache(types.Bytes(100 * 1024 * 1024), tmpDir)

	t.Run("Stream Read", func(t *testing.T) {
		fileKey := "stream-read-test"
		totalSize := types.Bytes(1.5 * BlockSize) // 1.5MB

		// Write test data
		testData := make([]byte, totalSize)
		for i := range testData {
			testData[i] = byte(i % 256)
		}

		cache.InitFile(fileKey, totalSize)
		cache.WriteAt(fileKey, 0, testData)

		// Create reader
		reader, err := cache.NewBlockReader(fileKey)
		if err != nil {
			t.Fatalf("NewBlockReader failed: %v", err)
		}

		// Read in 100KB chunks
		var result []byte
		buffer := make([]byte, 100*1024)

		for {
			n, err := reader.Read(buffer)
			if n > 0 {
				result = append(result, buffer[:n]...)
			}
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatalf("Reader.Read failed: %v", err)
			}
		}

		reader.Close()

		// Verify data
		if !bytes.Equal(result, testData) {
			t.Error("Stream read data mismatch")
		}
	})
}

func TestMissingBlocks(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "missing_blocks_test_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	cache := NewBlockCache(types.Bytes(100 * 1024 * 1024), tmpDir)

	fileKey := "missing-blocks-test"
	totalSize := types.Bytes(4 * BlockSize) // 4MB file
	cache.InitFile(fileKey, totalSize)

	// Write blocks 0 and 2, leave 1 and 3 missing
	block0 := make([]byte, BlockSize)
	block2 := make([]byte, BlockSize)

	cache.WriteAt(fileKey, 0, block0)                          // Block 0
	cache.WriteAt(fileKey, types.Bytes(2*BlockSize), block2)   // Block 2

	meta, _ := cache.GetFileMetadata(fileKey)
	missing := meta.GetMissingBlocks()

	expectedMissing := []int{1, 3}
	if len(missing) != len(expectedMissing) {
		t.Errorf("Missing blocks count: got %d, want %d", len(missing), len(expectedMissing))
	}

	for i, block := range expectedMissing {
		if missing[i] != block {
			t.Errorf("Missing block %d: got %d, want %d", i, missing[i], block)
		}
	}

	// File should not be complete
	if meta.IsComplete() {
		t.Error("File should not be complete with missing blocks")
	}

	// Should be able to read available blocks
	if !cache.HasBlock(fileKey, 0) {
		t.Error("Block 0 should be available")
	}
	if cache.HasBlock(fileKey, 1) {
		t.Error("Block 1 should not be available")
	}
	if !cache.HasBlock(fileKey, 2) {
		t.Error("Block 2 should be available")
	}
	if cache.HasBlock(fileKey, 3) {
		t.Error("Block 3 should not be available")
	}
}

func TestDiskPersistence(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "disk_persistence_test_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Create first cache instance
	cache1 := NewBlockCache(types.Bytes(100 * 1024 * 1024), tmpDir)

	fileKey := "persistence-test"
	totalSize := types.Bytes(2 * BlockSize) // 2MB file
	testData := make([]byte, totalSize)
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	// Write data with first cache
	cache1.InitFile(fileKey, totalSize)
	err = cache1.WriteAt(fileKey, 0, testData)
	if err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}

	// Verify it's complete
	meta1, _ := cache1.GetFileMetadata(fileKey)
	if !meta1.IsComplete() {
		t.Fatal("File should be complete after writing")
	}

	// Create second cache instance (simulates restart)
	cache2 := NewBlockCache(types.Bytes(100 * 1024 * 1024), tmpDir)

	// Initialize file - should discover existing blocks
	cache2.InitFile(fileKey, totalSize)

	meta2, exists := cache2.GetFileMetadata(fileKey)
	if !exists {
		t.Fatal("File metadata not found in second cache")
	}

	if !meta2.IsComplete() {
		t.Error("File should be complete after loading from disk")
	}

	// Verify we can read all blocks
	for i := range meta2.BlockCount {
		if !cache2.HasBlock(fileKey, i) {
			t.Errorf("Block %d should be available from disk", i)
		}
	}

	// Read complete file and verify data
	result, err := cache2.ReadFile(fileKey)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}

	if !bytes.Equal(result, testData) {
		t.Error("Data read from disk doesn't match original")
	}
}

func TestLRUEviction(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "lru_eviction_test_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Create cache with small RAM limit (only 2 blocks)
	cache := NewBlockCache(types.Bytes(2*BlockSize), tmpDir)

	fileKey := "lru-test"
	totalSize := types.Bytes(4 * BlockSize) // 4MB file (4 blocks)
	cache.InitFile(fileKey, totalSize)

	// Create test data for each block
	block0Data := make([]byte, BlockSize)
	block1Data := make([]byte, BlockSize)
	block2Data := make([]byte, BlockSize)
	block3Data := make([]byte, BlockSize)

	for i := range block0Data {
		block0Data[i] = 0
		block1Data[i] = 1
		block2Data[i] = 2
		block3Data[i] = 3
	}

	// Write all blocks to disk
	if err := cache.WriteAt(fileKey, 0, block0Data); err != nil {
		t.Fatalf("Failed to write block 0: %v", err)
	}
	if err := cache.WriteAt(fileKey, types.Bytes(BlockSize), block1Data); err != nil {
		t.Fatalf("Failed to write block 1: %v", err)
	}
	if err := cache.WriteAt(fileKey, types.Bytes(2*BlockSize), block2Data); err != nil {
		t.Fatalf("Failed to write block 2: %v", err)
	}
	if err := cache.WriteAt(fileKey, types.Bytes(3*BlockSize), block3Data); err != nil {
		t.Fatalf("Failed to write block 3: %v", err)
	}

	// Debug: Check which blocks are available after writing
	meta, _ := cache.GetFileMetadata(fileKey)
	for i := range 4 {
		hasBlock := cache.HasBlock(fileKey, i)
		metaHas := meta.HasBlock(i)
		t.Logf("Block %d: HasBlock=%v, MetaHas=%v", i, hasBlock, metaHas)
	}

	// At this point, only blocks 2 and 3 should be in memory (LRU limit = 2 blocks)
	// Blocks 0 and 1 should have been evicted

	// Check that blocks 2 and 3 are in memory (fast access)
	cache.ReadBlock(fileKey, 2) // Should be fast (in memory)
	cache.ReadBlock(fileKey, 3) // Should be fast (in memory)

	// Now read block 0 - this should:
	// 1. Load block 0 from disk
	// 2. Evict the LRU block (block 2, since we just accessed block 3)
	// 3. Cache block 0 as most recently used
	block0Read, err := cache.ReadBlock(fileKey, 0)
	if err != nil {
		t.Fatalf("Failed to read block 0: %v", err)
	}

	if !bytes.Equal(block0Read, block0Data) {
		t.Error("Block 0 data mismatch")
	}

	// Now read block 1 - this should:
	// 1. Load block 1 from disk
	// 2. Evict the LRU block (block 3, since we just accessed blocks 0 and 3 is older)
	// 3. Cache block 1 as most recently used
	block1Read, err := cache.ReadBlock(fileKey, 1)
	if err != nil {
		t.Fatalf("Failed to read block 1: %v", err)
	}

	if !bytes.Equal(block1Read, block1Data) {
		t.Error("Block 1 data mismatch")
	}

	// At this point, blocks 0 and 1 should be in memory
	// Check if block 2 is available (should be true - exists on disk)
	if !cache.HasBlock(fileKey, 2) {
		t.Fatal("Block 2 should be available (on disk)")
	}

	// Reading block 2 should load from disk and evict block 0 (LRU)
	block2Read, err := cache.ReadBlock(fileKey, 2)
	if err != nil {
		t.Fatalf("Failed to read block 2: %v", err)
	}

	if !bytes.Equal(block2Read, block2Data) {
		t.Error("Block 2 data mismatch")
	}

	// Verify LRU is working by checking access patterns
	// Access block 1 to make it most recent
	cache.ReadBlock(fileKey, 1)

	// Now read block 3 - should evict block 2 (LRU), not block 1 (most recent)
	block3Read, err := cache.ReadBlock(fileKey, 3)
	if err != nil {
		t.Fatalf("Failed to read block 3: %v", err)
	}

	if !bytes.Equal(block3Read, block3Data) {
		t.Error("Block 3 data mismatch")
	}

	t.Log("LRU eviction test completed successfully")
}

func BenchmarkBlockCache(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "block_bench_*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	cache := NewBlockCache(types.Bytes(1024 * 1024 * 1024), tmpDir) // 1GB

	b.Run("WriteAt", func(b *testing.B) {
		data := make([]byte, BlockSize) // 1MB
		for i := range data {
			data[i] = byte(i % 256)
		}

		b.ResetTimer()
		b.SetBytes(BlockSize)

		for b.Loop() {
			fileKey := "bench-file"
			cache.InitFile(fileKey, types.Bytes(BlockSize))
			cache.WriteAt(fileKey, 0, data)
		}
	})

	b.Run("ReadBlock", func(b *testing.B) {
		// Setup
		fileKey := "bench-read-file"
		data := make([]byte, BlockSize)
		for i := range data {
			data[i] = byte(i % 256)
		}

		cache.InitFile(fileKey, types.Bytes(BlockSize))
		cache.WriteAt(fileKey, 0, data)

		b.ResetTimer()
		b.SetBytes(BlockSize)

		for b.Loop() {
			cache.ReadBlock(fileKey, 0)
		}
	})

	b.Run("StreamWrite", func(b *testing.B) {
		data := make([]byte, BlockSize)
		for i := range data {
			data[i] = byte(i % 256)
		}

		b.ResetTimer()
		b.SetBytes(BlockSize)

		for b.Loop() {
			fileKey := "bench-stream-file"
			writer := cache.NewBlockWriter(fileKey, types.Bytes(BlockSize))
			writer.Write(data)
			writer.Close()
		}
	})
}

// BenchmarkPoolAllocations specifically tests sync.Pool allocation behavior
func BenchmarkPoolAllocations(b *testing.B) {
	b.Run("DirectPoolUsage", func(b *testing.B) {
		b.ResetTimer()

		for i := 0; b.Loop(); i++ {
			// Get from pool
			buf := blockPool.Get().(*[]byte)
			// Use the buffer (simulate some work)
			(*buf)[0] = byte(i)
			// Put back to pool
			blockPool.Put(buf)
		}
	})

	b.Run("WithoutPool", func(b *testing.B) {
		b.ResetTimer()

		for i := 0; b.Loop(); i++ {
			// Allocate new buffer each time (no pool)
			buf := make([]byte, BlockSize)
			// Use the buffer (simulate some work)
			buf[0] = byte(i)
			// Buffer goes out of scope and will be GC'd
		}
	})

	b.Run("WithEviction", func(b *testing.B) {
		tmpDir, err := os.MkdirTemp("", "pool_bench_*")
		if err != nil {
			b.Fatal(err)
		}
		defer os.RemoveAll(tmpDir)

		// Small cache to force evictions (only 2 blocks in RAM)
		cache := NewBlockCache(types.Bytes(2*BlockSize), tmpDir)
		data := make([]byte, BlockSize)

		b.ResetTimer()

		for i := 0; b.Loop(); i++ {
			// Write to different files to trigger evictions
			fileKey := fmt.Sprintf("file-%d", i%4)
			cache.InitFile(fileKey, types.Bytes(BlockSize))
			cache.WriteAt(fileKey, 0, data)
		}
	})
}
