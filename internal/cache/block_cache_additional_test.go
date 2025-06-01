package cache

import (
	"fmt"
	"os"
	"testing"

	"syncbit/internal/core/types"
)

func BenchmarkCacheHitVsMiss(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "hit_miss_bench_*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Small cache - only 4 blocks fit in RAM
	cache := NewBlockCache(types.Bytes(4*BlockSize), tmpDir)

	// Setup: Create file with 8 blocks, write all to disk
	fileKey := "hit-miss-test"
	totalSize := types.Bytes(8 * BlockSize) // 8MB file
	cache.InitFile(fileKey, totalSize)

	// Write all 8 blocks to disk
	for i := range 8 {
		blockData := make([]byte, BlockSize)
		for j := range blockData {
			blockData[j] = byte(i) // Each block has unique data
		}
		cache.WriteAt(fileKey, types.Bytes(i*BlockSize), blockData)
	}

	// At this point, only blocks 4,5,6,7 are in RAM (LRU limit = 4 blocks)
	// Blocks 0,1,2,3 are evicted to disk

	b.Run("CacheHit", func(b *testing.B) {
		b.ResetTimer()
		b.SetBytes(BlockSize)

		for i := 0; b.Loop(); i++ {
			// Read blocks that should be in cache (blocks 4-7)
			blockIndex := 4 + (i % 4)
			cache.ReadBlock(fileKey, blockIndex)
		}
	})

	b.Run("CacheMiss", func(b *testing.B) {
		b.ResetTimer()
		b.SetBytes(BlockSize)

		for i := 0; b.Loop(); i++ {
			// Read blocks that should NOT be in cache (blocks 0-3)
			// This will cause disk I/O + LRU eviction
			blockIndex := i % 4
			cache.ReadBlock(fileKey, blockIndex)
		}
	})
}

func BenchmarkMultiBlockOperations(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "multi_block_bench_*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	cache := NewBlockCache(types.Bytes(100*1024*1024), tmpDir) // 100MB RAM

	b.Run("Write10MB", func(b *testing.B) {
		data := make([]byte, 10*BlockSize) // 10MB = 10 blocks
		for i := range data {
			data[i] = byte(i % 256)
		}

		b.ResetTimer()
		b.SetBytes(int64(len(data)))

		for i := 0; b.Loop(); i++ {
			fileKey := fmt.Sprintf("file-%d", i)
			cache.InitFile(fileKey, types.Bytes(len(data)))
			cache.WriteAt(fileKey, 0, data)
		}
	})

	b.Run("Read10MB", func(b *testing.B) {
		// Setup: Pre-write a 10MB file
		fileKey := "read-test-file"
		data := make([]byte, 10*BlockSize)
		for i := range data {
			data[i] = byte(i % 256)
		}
		cache.InitFile(fileKey, types.Bytes(len(data)))
		cache.WriteAt(fileKey, 0, data)

		b.ResetTimer()
		b.SetBytes(int64(len(data)))

		for b.Loop() {
			cache.ReadFile(fileKey)
		}
	})
}

func BenchmarkRAMLimitStress(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "ram_stress_bench_*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Very small RAM limit - only 2 blocks fit
	cache := NewBlockCache(types.Bytes(2*BlockSize), tmpDir)

	// Create multiple files to stress the eviction
	numFiles := 5
	blocksPerFile := 4

	// Pre-setup files
	for fileNum := range numFiles {
		fileKey := fmt.Sprintf("stress-file-%d", fileNum)
		totalSize := types.Bytes(blocksPerFile * BlockSize)
		cache.InitFile(fileKey, totalSize)

		for blockNum := range blocksPerFile {
			blockData := make([]byte, BlockSize)
			for i := range blockData {
				blockData[i] = byte(fileNum*100 + blockNum)
			}
			cache.WriteAt(fileKey, types.Bytes(blockNum*BlockSize), blockData)
		}
	}

	b.Run("RandomAccess", func(b *testing.B) {
		b.ResetTimer()
		b.SetBytes(BlockSize)

		for i := 0; b.Loop(); i++ {
			// Random access pattern - should cause lots of evictions
			fileNum := i % numFiles
			blockNum := (i / numFiles) % blocksPerFile
			fileKey := fmt.Sprintf("stress-file-%d", fileNum)
			cache.ReadBlock(fileKey, blockNum)
		}
	})
}
