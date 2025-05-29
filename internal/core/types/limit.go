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
	return NewRateLimiter(DefaultRateLimit, DefaultRateBurst, DefaultConcurrency)
}

func NewRateLimiter(rateLimit, rateBurst Bytes, concurrency int) *RateLimiter {
	rateInt := rateLimit.Bytes()

	// If rate is 0, create an unlimited rate limiter
	if rateInt == 0 {
		return &RateLimiter{rate.NewLimiter(rate.Inf, 0)}
	}

	// Use rateBurst * concurrency as the burst size
	burstSize := int(rateBurst.Bytes()) * concurrency

	// Ensure burst is at least 1 byte and no more than rateInt/10
	if burstSize > int(rateInt/10) && rateInt > 0 {
		burstSize = int(rateInt / 10)
	}
	if burstSize < 1 {
		burstSize = 1
	}

	return &RateLimiter{rate.NewLimiter(rate.Limit(rateInt), burstSize)}
}
