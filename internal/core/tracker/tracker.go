package tracker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"syncbit/internal/core/types"

	"github.com/dustin/go-humanize"
)

// TrackerStatus is deprecated - use types.Status instead
type TrackerStatus = types.Status

// For backward compatibility
const (
	StatusPending   = types.StatusPending
	StatusRunning   = types.StatusRunning
	StatusSucceeded = types.StatusSucceeded
	StatusFailed    = types.StatusFailed
	StatusCanceled  = types.StatusCanceled
)

type Tracker struct {
	name      string
	mu        sync.RWMutex
	status    types.Status
	startedAt time.Time
	endedAt   time.Time
	current   int64
	total     int64
	err       error
}

func NewTracker(name string) *Tracker {
	t := &Tracker{
		name:      name,
		status:    types.StatusPending,
		startedAt: time.Time{},
		endedAt:   time.Time{},
	}
	return t
}

func (t *Tracker) Name() string {
	return t.name
}

func (t *Tracker) Status() types.Status {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.status
}

func (t *Tracker) Err() error {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.err
}

func (t *Tracker) StartedAt() time.Time {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.startedAt
}

func (t *Tracker) EndedAt() time.Time {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.endedAt
}

func (t *Tracker) Duration() time.Duration {
	t.mu.RLock()
	defer t.mu.RUnlock()
	switch t.status {
	case types.StatusPending:
		return time.Since(t.startedAt)
	case types.StatusRunning:
		return time.Since(t.startedAt)
	default:
		return t.endedAt.Sub(t.startedAt)
	}
}

func (t *Tracker) DurationString() string {
	return t.Duration().Round(time.Second).String()
}

func (t *Tracker) Current() int64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.current
}

func (t *Tracker) CurrentBytes() string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return humanize.Bytes(uint64(t.current))
}

func (t *Tracker) SetCurrent(current int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.current = max(0, current)
}

func (t *Tracker) IncCurrent(n int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.current = max(0, t.current+n)
}

func (t *Tracker) DecCurrent(n int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.current = max(0, t.current-n)
}

func (t *Tracker) Total() int64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.total
}

func (t *Tracker) TotalBytes() string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return humanize.Bytes(uint64(t.total))
}

func (t *Tracker) SetTotal(total int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.total = max(0, total)
}

func (t *Tracker) IncTotal(n int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.total = max(0, t.total+n)
}

func (t *Tracker) DecTotal(n int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.total = max(0, t.total-n)
}

// Progress returns the progress of the tracker current/total as a float from 0 to 1.
func (t *Tracker) Progress() float64 {
	if t.Total() == 0 {
		return 0
	}
	return float64(t.Current()) / float64(t.Total())
}

// ProgressFraction returns the progress of the tracker current/total as a string.
func (t *Tracker) ProgressFraction() string {
	return fmt.Sprintf("%d/%d", t.Current(), t.Total())
}

// ProgressBytes returns the progress of the tracker current/total as a human readable string.
func (t *Tracker) ProgressBytes() string {
	return fmt.Sprintf("%s/%s", t.CurrentBytes(), t.TotalBytes())
}

// Percent returns the progress of the tracker current/total as a float from 0 to 100.
func (t *Tracker) Percent() float64 {
	return t.Progress() * 100
}

// PercentString returns the progress of the tracker current/total as a string percentage.
func (t *Tracker) PercentString() string {
	return fmt.Sprintf("%.0f%%", t.Percent())
}

// Speed returns the average speed of the tracker current/duration as a float.
func (t *Tracker) Speed() float64 {
	duration := t.Duration().Seconds()
	if duration == 0 {
		return 0
	}
	return float64(t.Current()) / duration
}

// SpeedString returns the average speed of the tracker current/duration as a human readable string.
func (t *Tracker) SpeedString() string {
	return fmt.Sprintf("%.2f/s", t.Speed())
}

// SpeedBytes returns the average speed of the tracker current/duration as a human readable string.
func (t *Tracker) SpeedBytes() string {
	return fmt.Sprintf("%s/s", humanize.Bytes(uint64(t.Speed())))
}

// ETA returns the estimated time of arrival of the tracker current/total as a time.Duration.
func (t *Tracker) ETA() time.Duration {
	speed := t.Speed()
	if speed == 0 {
		return time.Duration(0)
	}
	delta := float64(t.Total() - t.Current())
	etaSeconds := delta / speed
	return time.Duration(etaSeconds) * time.Second
}

// ETAString returns the estimated time of arrival of the tracker current/total as a time.Duration string.
func (t *Tracker) ETAString() string {
	eta := t.ETA()
	if eta == 0 {
		return "unknown"
	}
	return eta.Round(time.Second).String()
}

// Start triggers the tracker to start.
func (t *Tracker) Start() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.startedAt = time.Now()
	t.status = types.StatusRunning
	t.err = nil
}

// Update updates the tracker from an error.
func (t *Tracker) Update(err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.endedAt = time.Now()
	switch err {
	case nil:
		t.status = types.StatusSucceeded
		t.err = nil
	case context.Canceled:
		t.status = types.StatusCanceled
		t.err = err
	case context.DeadlineExceeded:
		t.status = types.StatusFailed
		t.err = err
	default:
		t.status = types.StatusFailed
		t.err = err
	}
}

// Reset resets the tracker to its initial state.
func (t *Tracker) Reset() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.startedAt = time.Time{}
	t.endedAt = time.Time{}
	t.status = types.StatusPending
	t.err = nil
	t.current = 0
	t.total = 0
}

/// Status checks

func (t *Tracker) IsPending() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.status == types.StatusPending
}

func (t *Tracker) IsRunning() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.status == types.StatusRunning
}

func (t *Tracker) IsSucceeded() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.status == types.StatusSucceeded || t.status == types.StatusCompleted
}

func (t *Tracker) IsFailed() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.status == types.StatusFailed
}

func (t *Tracker) IsCanceled() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.status == types.StatusCanceled || t.status == types.StatusCancelled
}

func (t *Tracker) IsCompleted() bool {
	return t.Status().IsComplete()
}
