package cache

import (
	"sync"
	"time"

	"syncbit/internal/core/types"
)

// CacheMetrics tracks cache performance and usage statistics
type CacheMetrics struct {
	mu sync.RWMutex

	// Hit/Miss tracking
	totalRequests int64
	cacheHits     int64
	cacheMisses   int64

	// Timing metrics
	averageHitTime  time.Duration
	averageMissTime time.Duration
	hitTimes        []time.Duration
	missTimes       []time.Duration

	// Usage metrics
	totalBytesRead    types.Bytes
	totalBytesWritten types.Bytes
	totalBytesEvicted types.Bytes

	// Error tracking
	readErrors  int64
	writeErrors int64

	// Performance tracking
	evictionCount    int64
	compressionRatio float64

	startTime time.Time
}

// NewCacheMetrics creates a new cache metrics tracker
func NewCacheMetrics() *CacheMetrics {
	return &CacheMetrics{
		hitTimes:  make([]time.Duration, 0, 1000), // Keep last 1000 hit times
		missTimes: make([]time.Duration, 0, 1000), // Keep last 1000 miss times
		startTime: time.Now(),
	}
}

// RecordCacheHit records a cache hit with timing information
func (m *CacheMetrics) RecordCacheHit(duration time.Duration, bytesRead types.Bytes) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.totalRequests++
	m.cacheHits++
	m.totalBytesRead += bytesRead

	// Track timing
	m.hitTimes = append(m.hitTimes, duration)
	if len(m.hitTimes) > 1000 {
		m.hitTimes = m.hitTimes[1:] // Keep only last 1000 entries
	}
	m.updateAverageHitTime()
}

// RecordCacheMiss records a cache miss with timing information
func (m *CacheMetrics) RecordCacheMiss(duration time.Duration, bytesRead types.Bytes) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.totalRequests++
	m.cacheMisses++
	m.totalBytesRead += bytesRead

	// Track timing
	m.missTimes = append(m.missTimes, duration)
	if len(m.missTimes) > 1000 {
		m.missTimes = m.missTimes[1:] // Keep only last 1000 entries
	}
	m.updateAverageMissTime()
}

// RecordWrite records a cache write operation
func (m *CacheMetrics) RecordWrite(bytesWritten types.Bytes) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.totalBytesWritten += bytesWritten
}

// RecordEviction records a cache eviction
func (m *CacheMetrics) RecordEviction(bytesEvicted types.Bytes) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.evictionCount++
	m.totalBytesEvicted += bytesEvicted
}

// RecordReadError records a read error
func (m *CacheMetrics) RecordReadError() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.readErrors++
}

// RecordWriteError records a write error
func (m *CacheMetrics) RecordWriteError() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.writeErrors++
}

// updateAverageHitTime recalculates the average hit time (called with lock held)
func (m *CacheMetrics) updateAverageHitTime() {
	if len(m.hitTimes) == 0 {
		m.averageHitTime = 0
		return
	}

	var total time.Duration
	for _, duration := range m.hitTimes {
		total += duration
	}
	m.averageHitTime = total / time.Duration(len(m.hitTimes))
}

// updateAverageMissTime recalculates the average miss time (called with lock held)
func (m *CacheMetrics) updateAverageMissTime() {
	if len(m.missTimes) == 0 {
		m.averageMissTime = 0
		return
	}

	var total time.Duration
	for _, duration := range m.missTimes {
		total += duration
	}
	m.averageMissTime = total / time.Duration(len(m.missTimes))
}

// GetStats returns current cache performance statistics
func (m *CacheMetrics) GetStats() CachePerformanceStats {
	m.mu.RLock()
	defer m.mu.RUnlock()

	hitRatio := float64(0)
	if m.totalRequests > 0 {
		hitRatio = float64(m.cacheHits) / float64(m.totalRequests)
	}

	return CachePerformanceStats{
		TotalRequests:     m.totalRequests,
		CacheHits:         m.cacheHits,
		CacheMisses:       m.cacheMisses,
		HitRatio:          hitRatio,
		AverageHitTime:    m.averageHitTime,
		AverageMissTime:   m.averageMissTime,
		TotalBytesRead:    m.totalBytesRead,
		TotalBytesWritten: m.totalBytesWritten,
		TotalBytesEvicted: m.totalBytesEvicted,
		ReadErrors:        m.readErrors,
		WriteErrors:       m.writeErrors,
		EvictionCount:     m.evictionCount,
		CompressionRatio:  m.compressionRatio,
		Uptime:            time.Since(m.startTime),
	}
}

// Reset resets all metrics
func (m *CacheMetrics) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.totalRequests = 0
	m.cacheHits = 0
	m.cacheMisses = 0
	m.averageHitTime = 0
	m.averageMissTime = 0
	m.hitTimes = m.hitTimes[:0]
	m.missTimes = m.missTimes[:0]
	m.totalBytesRead = 0
	m.totalBytesWritten = 0
	m.totalBytesEvicted = 0
	m.readErrors = 0
	m.writeErrors = 0
	m.evictionCount = 0
	m.compressionRatio = 0
	m.startTime = time.Now()
}

// CachePerformanceStats represents detailed cache performance statistics
type CachePerformanceStats struct {
	TotalRequests     int64         `json:"total_requests"`
	CacheHits         int64         `json:"cache_hits"`
	CacheMisses       int64         `json:"cache_misses"`
	HitRatio          float64       `json:"hit_ratio"`
	AverageHitTime    time.Duration `json:"average_hit_time"`
	AverageMissTime   time.Duration `json:"average_miss_time"`
	TotalBytesRead    types.Bytes   `json:"total_bytes_read"`
	TotalBytesWritten types.Bytes   `json:"total_bytes_written"`
	TotalBytesEvicted types.Bytes   `json:"total_bytes_evicted"`
	ReadErrors        int64         `json:"read_errors"`
	WriteErrors       int64         `json:"write_errors"`
	EvictionCount     int64         `json:"eviction_count"`
	CompressionRatio  float64       `json:"compression_ratio"`
	Uptime            time.Duration `json:"uptime"`
}

// GetThroughputStats calculates throughput statistics
func (s *CachePerformanceStats) GetThroughputStats() ThroughputStats {
	uptimeSeconds := s.Uptime.Seconds()

	var avgRequestsPerSecond, avgBytesReadPerSecond, avgBytesWrittenPerSecond, errorRate float64
	if uptimeSeconds > 0 {
		avgRequestsPerSecond = float64(s.TotalRequests) / uptimeSeconds
		avgBytesReadPerSecond = float64(s.TotalBytesRead) / uptimeSeconds
		avgBytesWrittenPerSecond = float64(s.TotalBytesWritten) / uptimeSeconds
	}

	// Avoid division by zero for error rate
	if s.TotalRequests > 0 {
		errorRate = float64(s.ReadErrors+s.WriteErrors) / float64(s.TotalRequests)
	}

	return ThroughputStats{
		RequestsPerSecond:     avgRequestsPerSecond,
		BytesReadPerSecond:    avgBytesReadPerSecond,
		BytesWrittenPerSecond: avgBytesWrittenPerSecond,
		ErrorRate:             errorRate,
	}
}

// ThroughputStats represents throughput and performance metrics
type ThroughputStats struct {
	RequestsPerSecond     float64 `json:"requests_per_second"`
	BytesReadPerSecond    float64 `json:"bytes_read_per_second"`
	BytesWrittenPerSecond float64 `json:"bytes_written_per_second"`
	ErrorRate             float64 `json:"error_rate"`
}
