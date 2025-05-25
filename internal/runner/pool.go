package runner

import (
	"context"
	"fmt"
	"sync"

	"syncbit/internal/core/logger"
	"syncbit/internal/core/tracker"
)

const (
	poolSize    = 1000
	poolWorkers = 10
)

// PoolCallback is a callback that is called when the pool is done.
type PoolCallback func(ctx context.Context, pool *Pool)

// PoolMonitor is a monitor that is called in a goroutine when the pool is run.
type PoolMonitor func(ctx context.Context, pool *Pool)

// PoolOption is an option for a pool.
type PoolOption func(*Pool)

// WithPoolLogger is an option for a pool to set the logger.
func WithPoolLogger(logger *logger.Logger) PoolOption {
	return func(p *Pool) {
		p.logger = logger
	}
}

// WithPoolCallback is an option for a pool to set the callback.
func WithPoolCallback(callback PoolCallback) PoolOption {
	return func(p *Pool) {
		p.callback = callback
	}
}

// WithPoolMonitor is an option for a pool to set the monitor.
func WithPoolMonitor(monitor PoolMonitor) PoolOption {
	return func(p *Pool) {
		p.monitor = monitor
	}
}

// WithPoolQueueSize is an option for a pool to set the size of the queue.
func WithPoolQueueSize(size int) PoolOption {
	return func(p *Pool) {
		p.completed = make(chan *Job, size)
	}
}

// WithPoolWorkers is an option for a pool to set the number of workers.
func WithPoolWorkers(workers int) PoolOption {
	return func(p *Pool) {
		p.workers = workers
	}
}

// Pool is a pool of jobs.
type Pool struct {
	name      string
	workers   int
	wg        sync.WaitGroup
	logger    *logger.Logger
	tracker   *tracker.Tracker
	jobs      chan *Job
	completed chan *Job
	callback  PoolCallback
	monitor   PoolMonitor
	done      <-chan struct{}
}

// NewPool creates a new job pool with the given options.
func NewPool(ctx context.Context, name string, opts ...PoolOption) *Pool {
	p := &Pool{
		name:      name,
		workers:   poolWorkers,
		logger:    logger.NewLogger(logger.WithName(name)),
		tracker:   tracker.NewTracker(name),
		jobs:      make(chan *Job, poolSize),
		completed: make(chan *Job, poolSize),
		done:      ctx.Done(),
	}

	for _, opt := range opts {
		opt(p)
	}

	p.tracker.Start()
	p.startWorkers(ctx)

	// Start the monitor if it is set
	if p.monitor != nil {
		go p.monitor(ctx, p)
		p.logger.Debug("started pool monitor")
	}

	return p
}

func (p *Pool) startWorkers(ctx context.Context) {
	// Initialize waitgroup and start workers
	p.wg.Add(p.workers)
	for range p.workers {
		go p.worker(ctx)
	}
	// Start the finalizer
	go p.finalizer(ctx)
}

func (p *Pool) worker(ctx context.Context) {
	defer p.wg.Done()
	for {
		select {
		case <-p.done:
			p.logger.Debug("pool is closed, worker exiting")
			return
		case job := <-p.jobs:
			p.logger.Debug("worker running job", "job", job.Name())
			err := job.Run(ctx)
			p.tracker.Update(err)   // Update the tracker with the result
			p.tracker.IncCurrent(1) // Increment the completed job counter

			// Report the job to the completed channel
			select {
			case p.completed <- job:
				p.logger.Debug("job completed, reported to completed channel", "job", job.Name())
			default:
				p.logger.Warn(
					"job completed, but completed channel is full, logging results",
					"job", job.Name(),
					"status", job.Tracker().Status(),
					"duration", job.Tracker().DurationString(),
					"progress", job.Tracker().ProgressFraction(),
					"percent", job.Tracker().PercentString(),
					"speed", job.Tracker().SpeedString(),
					"error", job.Tracker().Err(),
				)
			}
		}
	}
}

func (p *Pool) finalizer(ctx context.Context) {
	p.wg.Wait()
	close(p.completed)

	// Invoke the callback if it is set
	if p.callback != nil {
		p.callback(ctx, p)
	}
}

// Submit adds new jobs to the pool.
func (p *Pool) Submit(job *Job) error {
	select {
	case <-p.done:
		p.logger.Debug("pool is closed, job rejected", "job", job.Name())
		return fmt.Errorf("pool is closed")
	default:
		select {
		case p.jobs <- job:
			p.logger.Debug("submitted job", "job", job.Name())
			p.tracker.IncTotal(1)
			return nil
		default:
			p.logger.Debug("jobs channel is full, job rejected", "job", job.Name())
			return fmt.Errorf("jobs channel is full")
		}
	}
}

// Wait blocks until a job is completed.
func (p *Pool) Wait() (job *Job, err error) {
	select {
	case job, ok := <-p.completed:
		if !ok {
			p.logger.Debug("completed channel is closed")
			return nil, fmt.Errorf("completed channel is closed")
		}
		p.logger.Debug("returning job", "job", job.Name())
		return job, nil
	case <-p.done:
		p.logger.Debug("pool is closed")
		return nil, fmt.Errorf("pool is closed")
	}
}

// Completed returns the completed job channel.
// The channel is closed when the pool is closed.
func (p *Pool) Completed() <-chan *Job {
	return p.completed
}

// Logger returns the logger of the pool.
func (p *Pool) Logger() *logger.Logger {
	return p.logger
}

// Tracker returns the tracker of the pool.
func (p *Pool) Tracker() *tracker.Tracker {
	return p.tracker
}
