package runner

import (
	"context"
	"fmt"

	"syncbit/internal/core/logger"
	"syncbit/internal/core/tracker"
)

// JobHandler is a handler that implements the job's behavior.
type JobHandler func(ctx context.Context, job *Job) error

// JobCallback is a callback function that is called when the job is done.
type JobCallback func(ctx context.Context, job *Job)

// JobMonitor is a monitor that is called in a goroutine when the job is run.
type JobMonitor func(ctx context.Context, job *Job)

// JobOption is an option for a job.
type JobOption func(job *Job)

// WithJobLogger is an option for a job to set the logger.
func WithJobLogger(logger *logger.Logger) JobOption {
	return func(j *Job) {
		j.logger = logger
	}
}

// WithJobCallback is an option for a job to set the callback.
func WithJobCallback(callback JobCallback) JobOption {
	return func(j *Job) {
		j.callback = callback
	}
}

// WithJobMonitor is an option for a job to set the monitor.
func WithJobMonitor(monitor JobMonitor) JobOption {
	return func(j *Job) {
		j.monitor = monitor
	}
}

// Job is a job that runs tasks in order.
type Job struct {
	logger   *logger.Logger
	tracker  *tracker.Tracker
	name     string
	callback JobCallback
	monitor  JobMonitor
	handler  JobHandler
}

// NewJob creates a new job with a name and a handler.
func NewJob(name string, handler JobHandler, opts ...JobOption) *Job {
	j := &Job{
		logger:  logger.NewLogger(logger.WithName(name)),
		tracker: tracker.NewTracker(name),
		name:    name,
		handler: handler,
	}
	for _, opt := range opts {
		opt(j)
	}

	return j
}

// Name returns the name of the job.
func (j Job) Name() string {
	return j.name
}

// Run runs the job handler.
func (j *Job) Run(ctx context.Context) error {
	// Preflight check
	if !j.tracker.IsPending() {
		j.logger.Debug("job already started", "job", j.name)
		return fmt.Errorf("job already started")
	}
	j.tracker.Start() // Start the tracker

	// Start the monitor if it is set
	if j.monitor != nil {
		go j.monitor(ctx, j)
		j.logger.Debug("started job monitor", "job", j.name)
	}

	// Run the handler
	err := j.handler(ctx, j)
	j.tracker.Update(err) // Update the tracker with the result

	// Invoke the callback if it is set
	if j.callback != nil {
		j.callback(ctx, j)
		j.logger.Debug("invoked job callback", "job", j.name)
	}

	return err
}

// Tracker returns the tracker of the job.
func (j Job) Tracker() *tracker.Tracker {
	return j.tracker
}

// Logger returns the logger of the job.
func (j Job) Logger() *logger.Logger {
	return j.logger
}
