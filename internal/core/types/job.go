package types

// JobStatus is deprecated - use Status instead
type JobStatus = Status

// For backward compatibility
const (
	JobStatusPending   = StatusPending
	JobStatusRunning   = StatusRunning
	JobStatusCompleted = StatusCompleted
	JobStatusFailed    = StatusFailed
	JobStatusCancelled = StatusCancelled
)

// JobHandler represents the handler type for a job
type JobHandler string

const (
	JobHandlerHF   JobHandler = "hf"
	JobHandlerS3   JobHandler = "s3"
	JobHandlerHTTP JobHandler = "http"
	JobHandlerP2P  JobHandler = "p2p"
)

// JobConfig represents the configuration for a download job
type JobConfig struct {
	ProviderID string   `json:"provider_id"`
	Repo       string   `json:"repo"`
	Revision   string   `json:"revision"`
	Files      []string `json:"files"`
	LocalPath  string   `json:"local_path"`
}

// Job represents a download job
type Job struct {
	ID      string     `json:"id"`
	Handler JobHandler `json:"handler"`
	Config  JobConfig  `json:"config"`
	Status  Status     `json:"status"`
	Error   string     `json:"error,omitempty"`
}

// NewJob creates a new job with default values
func NewJob(id string, handler JobHandler, config JobConfig) *Job {
	return &Job{
		ID:      id,
		Handler: handler,
		Config:  config,
		Status:  StatusPending,
	}
}

// IsActive returns true if the job is currently active (pending or running)
func (j *Job) IsActive() bool {
	return j.Status.IsActive()
}

// IsComplete returns true if the job has finished (completed, failed, or cancelled)
func (j *Job) IsComplete() bool {
	return j.Status.IsComplete()
}

// SetStatus updates the job status
func (j *Job) SetStatus(status Status, errorMsg string) {
	j.Status = status.Normalize()
	j.Error = errorMsg
}
