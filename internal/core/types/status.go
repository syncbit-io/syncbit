package types

// Status represents the status of any trackable operation (jobs, tasks, transfers, etc.)
type Status string

const (
	StatusPending   Status = "pending"
	StatusRunning   Status = "running"
	StatusCompleted Status = "completed" // For jobs/operations that finished successfully
	StatusSucceeded Status = "succeeded" // Alias for completed (for tracker compatibility)
	StatusFailed    Status = "failed"
	StatusCancelled Status = "cancelled"
	StatusCanceled  Status = "canceled" // Alias for cancelled (for tracker compatibility)
)

// IsActive returns true if the status indicates an ongoing operation
func (s Status) IsActive() bool {
	return s == StatusPending || s == StatusRunning
}

// IsComplete returns true if the status indicates a finished operation
func (s Status) IsComplete() bool {
	return s == StatusCompleted || s == StatusSucceeded || s == StatusFailed || s == StatusCancelled || s == StatusCanceled
}

// IsSuccess returns true if the status indicates successful completion
func (s Status) IsSuccess() bool {
	return s == StatusCompleted || s == StatusSucceeded
}

// IsFailure returns true if the status indicates failure or cancellation
func (s Status) IsFailure() bool {
	return s == StatusFailed || s == StatusCancelled || s == StatusCanceled
}

// Normalize returns the canonical form of the status
func (s Status) Normalize() Status {
	switch s {
	case StatusSucceeded:
		return StatusCompleted
	case StatusCanceled:
		return StatusCancelled
	default:
		return s
	}
}
