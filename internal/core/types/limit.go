package types

import (
	"github.com/dustin/go-humanize"
	"golang.org/x/time/rate"
)

const (
	DefaultConcurrency = 10
	DefaultRateLimit   = 1 * humanize.GByte // 1GB/s
	DefaultRateBurst   = 1 * humanize.MByte // 1MB * concurrency
)

type RateLimiter struct {
	*rate.Limiter
}

func DefaultRateLimiter() *RateLimiter {
	return NewRateLimiter(DefaultRateLimit)
}

// NewRateLimiter creates a rate limiter with intelligent burst size calculation
func NewRateLimiter(rateLimit Bytes) *RateLimiter {
	rateInt := rateLimit.Bytes()

	// If rate is 0, create an unlimited rate limiter
	if rateInt == 0 {
		return &RateLimiter{rate.NewLimiter(rate.Inf, 0)}
	}

	// Calculate intelligent burst size: 1% of rate limit with reasonable bounds
	burstSize := int(rateInt / 100)
	
	// Minimum burst of 64KB for small rate limits
	if burstSize < 64*1024 {
		burstSize = 64 * 1024
	}
	
	// Maximum burst of 10MB for very high rate limits
	if burstSize > 10*1024*1024 {
		burstSize = 10 * 1024 * 1024
	}

	return &RateLimiter{rate.NewLimiter(rate.Limit(rateInt), burstSize)}
}
