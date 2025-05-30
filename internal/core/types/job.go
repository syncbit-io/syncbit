package types

import (
	"fmt"
)

// JobHandler represents the handler type for a job
type JobHandler string

const (
	JobHandlerDownload JobHandler = "download" // Unified download handler
)

// ProviderSource represents a single source for downloading a file (peer or upstream provider)
type ProviderSource struct {
	ProviderID string  `json:"provider_id"`         // ID of the provider to use
	PeerAddr   Address `json:"peer_addr,omitempty"` // For peer providers, the agent address
}

// DistributionRequest specifies how a job should be distributed across agents
type DistributionRequest struct {
	Strategy     string   `json:"strategy"`                // "single", "all", "count", "specific"
	TargetAgents []string `json:"target_agents,omitempty"` // for "specific" strategy
	TargetCount  int      `json:"target_count,omitempty"`  // for "count" strategy
}

// DefaultDistributionRequest returns a single-agent distribution (backward compatibility)
func DefaultDistributionRequest() DistributionRequest {
	return DistributionRequest{
		Strategy: "single",
	}
}

// JobConfig represents the configuration for downloading a single file from a single provider
type JobConfig struct {
	Repo           string              `json:"repo"`            // Repository (e.g., "meta-llama/Llama-2-7b-hf")
	Revision       string              `json:"revision"`        // Repository revision (e.g., "main", "v1.0")
	FilePath       string              `json:"file_path"`       // Single file to download
	LocalPath      string              `json:"local_path"`      // Local destination path
	ProviderSource ProviderSource      `json:"provider_source"` // Single provider to use (no fallback)
	Distribution   DistributionRequest `json:"distribution"`    // Distribution requirements (for reconciliation)
}

// Job represents a single-file download job from a single provider
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

// IsPeerJob returns true if this job uses a peer provider
func (j *Job) IsPeerJob() bool {
	return j.Config.ProviderSource.ProviderID == "peer-main"
}

// IsUpstreamJob returns true if this job uses an upstream provider (non-peer)
func (j *Job) IsUpstreamJob() bool {
	return !j.IsPeerJob()
}

// GetFileKey returns a unique key for the file this job is downloading
func (j *Job) GetFileKey() string {
	return fmt.Sprintf("%s|%s|%s", j.Config.Repo, j.Config.Revision, j.Config.FilePath)
}

// GetDatasetName returns the dataset name for this job
func (j *Job) GetDatasetName() string {
	return fmt.Sprintf("%s-%s", j.Config.Repo, j.Config.Revision)
}
