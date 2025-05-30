package types

import "time"

// AgentState represents the current state reported by an agent
type AgentState struct {
	DiskUsed      Bytes     `json:"disk_used"`      // Total disk usage in bytes
	DiskAvailable Bytes     `json:"disk_available"` // Available disk space in bytes
	ActiveJobs    []string  `json:"active_jobs"`    // Currently running job IDs
	LastUpdated   time.Time `json:"last_updated"`   // When this state was last updated
}

// Agent represents a registered agent
type Agent struct {
	ID            string     `json:"id"`             // Unique agent identifier
	AdvertiseAddr Address    `json:"advertise_addr"` // Address other agents can reach this one at
	LastHeartbeat time.Time  `json:"last_heartbeat"` // Last time we heard from this agent
	State         AgentState `json:"state"`          // Current agent state
}

// IsHealthy returns true if the agent has sent a heartbeat recently
func (a *Agent) IsHealthy(maxAge time.Duration) bool {
	return time.Since(a.LastHeartbeat) <= maxAge
}

// HasCapacity returns true if the agent has sufficient disk space for new jobs
func (a *Agent) HasCapacity(requiredSpace Bytes) bool {
	return a.State.DiskAvailable >= requiredSpace
}

// IsIdle returns true if the agent has no active jobs
func (a *Agent) IsIdle() bool {
	return len(a.State.ActiveJobs) == 0
}

// GetLoad returns the number of active jobs as a load indicator
func (a *Agent) GetLoad() int {
	return len(a.State.ActiveJobs)
}
