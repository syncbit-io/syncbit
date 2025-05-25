// Provides helper functions for working with contexts.
package types

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// NewSubContext creates a new sub-context of the given context.
func NewSubContext(ctx context.Context) context.Context {
	return context.WithoutCancel(ctx)
}

// NewCancelableSubContext creates a new cancellable sub-context of the given context.
func NewCancelableSubContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithCancel(ctx)
}

// NewTimeoutSubContext creates a new cancellable sub-context that is cancelled when the provided timeout is reached.
func NewTimeoutSubContext(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, timeout)
}

// NewSignalNotifySubContext creates a new cancellable sub-context that is cancelled when the provided signals are received.
func NewSignalNotifySubContext(ctx context.Context, signals ...os.Signal) (context.Context, context.CancelFunc) {
	return signal.NotifyContext(ctx, signals...)
}

// DefaultContext creates a new background context.
func DefaultContext() context.Context {
	return context.Background()
}

// DefaultCancelableContext creates a new cancelable context.
func DefaultCancelableContext() (context.Context, context.CancelFunc) {
	return context.WithCancel(DefaultContext())
}

// DefaultSignalNotifySubContext creates a new cancellable sub-context that is cancelled when the default signals (SIGINT and SIGTERM) are received.
func DefaultSignalNotifySubContext() (context.Context, context.CancelFunc) {
	return NewSignalNotifySubContext(DefaultContext(), os.Interrupt, syscall.SIGTERM)
}
