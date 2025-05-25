package transfer

import (
	"syncbit/internal/core/types"

	"github.com/dustin/go-humanize"
	"golang.org/x/time/rate"
)

const (
	DefaultConcurrency = 10
	DefaultRateLimit   = 1 * humanize.GByte // 1GB/s
	DefaultRateBurst   = 1 * humanize.MByte // 1MB * concurrency
)

func DefaultRateLimiter() *rate.Limiter {
	return NewRateLimiter(DefaultRateLimit, DefaultRateBurst, DefaultConcurrency)
}

func NewRateLimiter(rateLimit, rateBurst types.Bytes, concurrency int) *rate.Limiter {
	rateInt := rateLimit.Bytes()

	// If rate is 0, create an unlimited rate limiter
	if rateInt == 0 {
		return rate.NewLimiter(rate.Inf, 0)
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

	return rate.NewLimiter(rate.Limit(rateInt), burstSize)
}
