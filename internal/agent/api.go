package agent

import (
	"encoding/json"
	"net/http"

	"syncbit/internal/api"
	"syncbit/internal/api/response"
)

// RegisterHandlers registers the handlers for the agent.
func (a *Agent) RegisterHandlers(registrar api.HandlerRegistrar) error {
	// Health endpoint
	registrar.RegisterHandler(api.NewRoute(api.MethodGet, "/health", a.handleHealth))

	// Agent state and information endpoints
	registrar.RegisterHandler(api.NewRoute(api.MethodGet, "/state", a.handleGetState))
	registrar.RegisterHandler(api.NewRoute(api.MethodGet, "/info", a.handleGetInfo))

	// Job management endpoints for controllers to assign work
	registrar.RegisterHandler(api.NewRoute(api.MethodPost, "/jobs", a.handleSubmitJob))
	registrar.RegisterHandler(api.NewRoute(api.MethodGet, "/jobs/{id}", a.handleGetJob))
	registrar.RegisterHandler(api.NewRoute(api.MethodPost, "/jobs/{id}/cancel", a.handleCancelJob))

	// File and dataset endpoints for peer-to-peer transfer
	registrar.RegisterHandler(api.NewRoute(api.MethodGet, "/datasets", a.handleListDatasets))
	registrar.RegisterHandler(api.NewRoute(api.MethodGet, "/datasets/{name}/files", a.handleListFiles))
	registrar.RegisterHandler(api.NewRoute(api.MethodGet, "/datasets/{name}/files/{path}", a.handleGetFile))

	// Block-level endpoints for efficient peer-to-peer transfer
	registrar.RegisterHandler(api.NewRoute(api.MethodGet, "/blocks/{hash}", a.handleGetBlock))
	registrar.RegisterHandler(api.NewRoute(api.MethodHead, "/blocks/{hash}", a.handleHeadBlock))

	return nil
}

// handleHealth is the handler for the health endpoint.
func (a *Agent) handleHealth(w http.ResponseWriter, r *http.Request) {
	response.Respond(w,
		response.WithString("OK"),
	)
}

// handleGetState returns the current agent state
func (a *Agent) handleGetState(w http.ResponseWriter, r *http.Request) {
	// TODO: Implement proper state collection
	state := AgentState{
		DiskUsed:      0,
		DiskAvailable: 1024 * 1024 * 1024 * 1024, // 1TB placeholder
		DataSets:      []DataSetInfo{},
		ActiveJobs:    []string{},
	}

	var payload response.JSON = make(response.JSON)
	payload["state"] = state

	response.Respond(w,
		response.WithJSON(payload),
	)
}

// handleGetInfo returns agent information
func (a *Agent) handleGetInfo(w http.ResponseWriter, r *http.Request) {
	var payload response.JSON = make(response.JSON)
	payload["id"] = a.cfg.Agent.ID
	payload["advertise_addr"] = a.cfg.Agent.Network.AdvertiseAddr.URL()
	payload["listen_addr"] = a.cfg.Agent.Network.ListenAddr.URL()

	response.Respond(w,
		response.WithJSON(payload),
	)
}

// handleSubmitJob handles job submission from controllers
func (a *Agent) handleSubmitJob(w http.ResponseWriter, r *http.Request) {
	var job Job
	if err := json.NewDecoder(r.Body).Decode(&job); err != nil {
		a.log.Error("Failed to decode job", "error", err)
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	// TODO: Implement job submission to local job queue/pool
	a.log.Info("Received job from controller", "job_id", job.ID, "type", job.Type)

	var payload response.JSON = make(response.JSON)
	payload["message"] = "Job accepted"
	payload["job_id"] = job.ID
	payload["status"] = "accepted"

	response.Respond(w,
		response.WithJSONStatus(payload, http.StatusAccepted),
	)
}

// handleGetJob returns job status
func (a *Agent) handleGetJob(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("id")
	if jobID == "" {
		http.Error(w, "Job ID is required", http.StatusBadRequest)
		return
	}

	// TODO: Implement job status lookup
	var payload response.JSON = make(response.JSON)
	payload["job_id"] = jobID
	payload["status"] = "unknown"

	response.Respond(w,
		response.WithJSON(payload),
	)
}

// handleCancelJob cancels a running job
func (a *Agent) handleCancelJob(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("id")
	if jobID == "" {
		http.Error(w, "Job ID is required", http.StatusBadRequest)
		return
	}

	// TODO: Implement job cancellation
	a.log.Info("Job cancellation requested", "job_id", jobID)

	var payload response.JSON = make(response.JSON)
	payload["message"] = "Job cancellation requested"
	payload["job_id"] = jobID

	response.Respond(w,
		response.WithJSON(payload),
	)
}

// handleListDatasets returns available datasets on this agent
func (a *Agent) handleListDatasets(w http.ResponseWriter, r *http.Request) {
	// TODO: Implement dataset discovery from storage
	datasets := []DataSetInfo{}

	var payload response.JSON = make(response.JSON)
	payload["datasets"] = datasets
	payload["count"] = len(datasets)

	response.Respond(w,
		response.WithJSON(payload),
	)
}

// handleListFiles returns files in a specific dataset
func (a *Agent) handleListFiles(w http.ResponseWriter, r *http.Request) {
	datasetName := r.PathValue("name")
	if datasetName == "" {
		http.Error(w, "Dataset name is required", http.StatusBadRequest)
		return
	}

	// TODO: Implement file listing for dataset
	files := []FileInfo{}

	var payload response.JSON = make(response.JSON)
	payload["dataset"] = datasetName
	payload["files"] = files
	payload["count"] = len(files)

	response.Respond(w,
		response.WithJSON(payload),
	)
}

// handleGetFile serves a file from a dataset
func (a *Agent) handleGetFile(w http.ResponseWriter, r *http.Request) {
	datasetName := r.PathValue("name")
	filePath := r.PathValue("path")

	if datasetName == "" || filePath == "" {
		http.Error(w, "Dataset name and file path are required", http.StatusBadRequest)
		return
	}

	// TODO: Implement file serving with range support
	a.log.Debug("File requested", "dataset", datasetName, "file", filePath)

	http.Error(w, "File serving not yet implemented", http.StatusNotImplemented)
}

// handleGetBlock serves a specific block by hash
func (a *Agent) handleGetBlock(w http.ResponseWriter, r *http.Request) {
	blockHash := r.PathValue("hash")
	if blockHash == "" {
		http.Error(w, "Block hash is required", http.StatusBadRequest)
		return
	}

	// TODO: Implement block serving for peer-to-peer transfer
	a.log.Debug("Block requested", "hash", blockHash)

	http.Error(w, "Block serving not yet implemented", http.StatusNotImplemented)
}

// handleHeadBlock checks if a block exists without transferring content
func (a *Agent) handleHeadBlock(w http.ResponseWriter, r *http.Request) {
	blockHash := r.PathValue("hash")
	if blockHash == "" {
		http.Error(w, "Block hash is required", http.StatusBadRequest)
		return
	}

	// TODO: Implement block existence check
	a.log.Debug("Block existence check", "hash", blockHash)

	http.Error(w, "Block checking not yet implemented", http.StatusNotImplemented)
}

// AgentState represents the current state reported by an agent
type AgentState struct {
	DiskUsed      int64         `json:"disk_used"`      // Total disk usage in bytes
	DiskAvailable int64         `json:"disk_available"` // Available disk space in bytes
	DataSets      []DataSetInfo `json:"datasets"`       // What datasets this agent has
	ActiveJobs    []string      `json:"active_jobs"`    // Currently running job IDs
}

// DataSetInfo represents information about a dataset on an agent
type DataSetInfo struct {
	Name  string     `json:"name"`  // Dataset name (subdirectory)
	Files []FileInfo `json:"files"` // Files in this dataset
	Size  int64      `json:"size"`  // Total size of dataset in bytes
}

// FileInfo represents information about a file on an agent
type FileInfo struct {
	Path         string `json:"path"`          // Relative path within dataset
	Size         int64  `json:"size"`          // File size in bytes
	Status       string `json:"status"`        // "complete", "downloading", "partial"
	BlocksTotal  int    `json:"blocks_total"`  // Total number of blocks
	BlocksHave   int    `json:"blocks_have"`   // Number of blocks we have
	LastModified string `json:"last_modified"` // Last modification time
}
