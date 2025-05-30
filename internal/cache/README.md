# SyncBit Cache System

The SyncBit cache system provides efficient file-level caching for peer-to-peer file synchronization, optimized for storing complete files directly in their target locations on disk.

## Architecture Overview

The cache system is designed around the principle that **files should be stored in their final locations** (`/{basedir}/{dataset_name}/{file_path}`) as complete units, rather than managing complex block-level operations. This simplified approach:

- **Minimizes complexity** by treating files as atomic units
- **Optimizes for the primary goal** of having complete files ready for GPU loading
- **Supports efficient P2P synchronization** through file-level operations
- **Provides intelligent caching** with simple LRU eviction for performance
- **Reduces coordination overhead** by eliminating block-level tracking

## Key Components

### FileStorage Interface
- **Purpose**: Manages actual files on disk at their target locations
- **Operations**: WriteFile/ReadFile for complete file operations
- **Implementation**: `DiskFileStorage` uses OS file operations with atomic writes

### Cache Layer
- **RAM Cache**: In-memory file cache with LRU eviction
- **File Index**: Metadata tracking for file completion status
- **Write-Through**: Complete files are immediately written to disk
- **Read-Through**: Missing files are loaded from disk into RAM cache

### LRU Eviction Strategy
- **Simple and Fast**: Least Recently Used eviction based on file access
- **Size-Based**: Tracks file sizes for memory management
- **Configurable**: RAM limits can be adjusted for different workloads

## File-Based Storage

### How It Works

1. **File Storage**: When a file is stored via `StoreFile()`:
   - Complete file is cached in RAM for fast access
   - File is written atomically to disk at final location
   - Metadata is updated to track file completion

2. **File Retrieval**: When a file is requested via `GetFile()`:
   - RAM cache is checked first (fast path)
   - If not in RAM, file is read from disk
   - File is loaded back into RAM cache if space permits

3. **File Completion**: Files are either complete or not present:
   - No partial file states to manage
   - No block assembly or coordination required
   - Other processes can immediately access complete files

### Directory Structure

```
{basePath}/
├── {dataset1}/
│   ├── model.safetensors      # Complete model file
│   ├── tokenizer.json         # Complete tokenizer file
│   └── config.json           # Complete config file
└── {dataset2}/
    └── model.bin             # Another model file
```

## Usage Examples

### Basic File Operations

```go
// Create cache with file storage
cache, err := NewCache(config, "/path/to/storage")

// Store complete files
fileKey, err := cache.StoreFile("llama-7b", "model.safetensors", fileData)

// Retrieve files (read-through caching)
data, err := cache.GetFileByPath("llama-7b", "model.safetensors")

// Check file availability
exists := cache.HasFileByPath("llama-7b", "model.safetensors")
```

### File-Level Operations

```go
// Create file reader/writer interfaces
reader := cache.NewFileReader("llama-7b", "model.safetensors", fileSize)
writer := cache.NewFileWriter("llama-7b", "model.safetensors", fileSize)

// Use standard io.Reader/io.Writer interfaces
data := make([]byte, 1024)
n, err := reader.Read(data)

// Write complete files
n, err := writer.Write(fileData)
writer.Close() // Stores to cache and disk
```

### Integration with ReaderWriter

```go
// Create ReaderWriter for unified access
rw := cache.NewReaderWriter("llama-7b", "model.safetensors", fileSize)

// Use with standard I/O patterns
reader := rw.Reader(ctx)
writer := rw.Writer(ctx)
```

## Performance Characteristics

### Memory Usage
- **RAM Cache**: Configurable limit with LRU eviction
- **File Metadata**: Minimal overhead for tracking file status
- **No Duplication**: Files exist either in RAM or on disk, never both unnecessarily

### Disk Usage
- **Optimal**: Files stored once in their final locations
- **Atomic Writes**: Files are written atomically using temp files and rename
- **No Temporary Storage**: No separate cache directory

### Access Patterns
- **Hot Files**: Frequently accessed files stay in RAM
- **Cold Files**: Infrequently accessed files are evicted but remain on disk
- **Simple Operations**: All file operations are atomic and straightforward

## Configuration

```go
type CacheConfig struct {
    RAMLimit        types.Bytes // Maximum RAM usage for file cache
    DiskLimit       types.Bytes // Maximum disk usage (for future use)
}
```

## Benefits for SyncBit

1. **Efficient P2P Sync**: Files can be transferred as complete units
2. **Minimal Disk Usage**: No duplicate storage of file data
3. **Fast GPU Loading**: Files are ready in their final locations
4. **Simple Caching**: LRU keeps recently used files in RAM for performance
5. **Easy P2P Integration**: Agents either have a file or they don't
6. **Reduced Complexity**: No block coordination or partial file states
7. **Better Performance**: Eliminates block-level overhead for ≤5GB files

## P2P Integration

With file-level caching, P2P operations become much simpler:

- **Controller tracking**: Tracks which agents have which complete files
- **Job assignment**: "Download file X, these agents have it: [agent1, agent2]"
- **Agent operations**: Try peers in order, fall back to origin if needed
- **No coordination**: No need to track block availability or partial transfers

This architecture ensures that the cache system serves its primary purpose: getting complete, usable files onto disk as efficiently as possible while providing the file-level abstractions needed for effective peer-to-peer synchronization.
