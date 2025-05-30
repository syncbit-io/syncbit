package cache

import (
	"container/list"
	"sync"

	"syncbit/internal/core/types"
)

// LRUCache provides a simple Least Recently Used cache implementation
type LRUCache struct {
	mu        sync.RWMutex
	capacity  types.Bytes
	usage     types.Bytes
	items     map[string]*list.Element
	evictList *list.List
}

// LRUEntry represents an entry in the LRU cache
type LRUEntry struct {
	key  string
	size types.Bytes
}

// LRUStats provides statistics about the LRU cache
type LRUStats struct {
	HitRate       float64     `json:"hit_rate"`
	TotalAccesses int64       `json:"total_accesses"`
	Hits          int64       `json:"hits"`
	Misses        int64       `json:"misses"`
	Evictions     int64       `json:"evictions"`
	CurrentSize   int         `json:"current_size"`
	CurrentUsage  types.Bytes `json:"current_usage"`
	Capacity      types.Bytes `json:"capacity"`
}

// NewLRUCache creates a new LRU cache with the specified capacity
func NewLRUCache(capacity types.Bytes) *LRUCache {
	return &LRUCache{
		capacity:  capacity,
		usage:     0,
		items:     make(map[string]*list.Element),
		evictList: list.New(),
	}
}

// Add adds an item to the cache with the specified size
func (c *LRUCache) Add(key string, size types.Bytes) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// If item already exists, move to front and update size
	if elem, exists := c.items[key]; exists {
		entry := elem.Value.(*LRUEntry)
		c.usage = c.usage - entry.size + size
		entry.size = size
		c.evictList.MoveToFront(elem)
		return
	}

	// Evict items if necessary
	c.evictToMakeRoom(size)

	// Add new item
	entry := &LRUEntry{
		key:  key,
		size: size,
	}
	elem := c.evictList.PushFront(entry)
	c.items[key] = elem
	c.usage += size
}

// Access marks an item as recently used (moves to front)
func (c *LRUCache) Access(key string, size types.Bytes) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, exists := c.items[key]; exists {
		c.evictList.MoveToFront(elem)
	}
}

// Remove removes an item from the cache
func (c *LRUCache) Remove(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, exists := c.items[key]; exists {
		entry := elem.Value.(*LRUEntry)
		c.usage -= entry.size
		delete(c.items, key)
		c.evictList.Remove(elem)
	}
}

// GetLeastValuable returns the least recently used item key
func (c *LRUCache) GetLeastValuable() (string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.evictList.Len() == 0 {
		return "", false
	}

	elem := c.evictList.Back()
	entry := elem.Value.(*LRUEntry)
	return entry.key, true
}

// evictToMakeRoom evicts items until there's enough space for the new item
func (c *LRUCache) evictToMakeRoom(neededSize types.Bytes) {
	for c.usage+neededSize > c.capacity && c.evictList.Len() > 0 {
		elem := c.evictList.Back()
		entry := elem.Value.(*LRUEntry)

		c.usage -= entry.size
		delete(c.items, entry.key)
		c.evictList.Remove(elem)
	}
}

// GetStats returns statistics about the LRU cache
func (c *LRUCache) GetStats() LRUStats {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return LRUStats{
		HitRate:       0.0, // Simple LRU doesn't track hit rate
		TotalAccesses: 0,
		Hits:          0,
		Misses:        0,
		Evictions:     0,
		CurrentSize:   len(c.items),
		CurrentUsage:  c.usage,
		Capacity:      c.capacity,
	}
}

// Clear removes all items from the cache
func (c *LRUCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.items = make(map[string]*list.Element)
	c.evictList = list.New()
	c.usage = 0
}
