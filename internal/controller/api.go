package controller

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"syncbit/internal/api"
	"syncbit/internal/api/response"
)

// RegisterHandlers registers the handlers for the controller.
func (c *Controller) RegisterHandlers(registrar api.HandlerRegistrar) error {
	// Health endpoint
	registrar.RegisterHandler(api.NewRoute(api.MethodGet, "/health", c.handleHealth))

	// Job management endpoints using Go 1.22+ path parameters
	registrar.RegisterHandler(api.NewRoute(api.MethodGet, "/jobs", c.handleListJobs))
	registrar.RegisterHandler(api.NewRoute(api.MethodPost, "/jobs", c.handleSubmitJob))
	registrar.RegisterHandler(api.NewRoute(api.MethodGet, "/jobs/{id}", c.handleGetJob))

	// Agent endpoints
	registrar.RegisterHandler(api.NewRoute(api.MethodGet, "/jobs/next", c.handleGetNextJob))
	registrar.RegisterHandler(api.NewRoute(api.MethodPost, "/jobs/{id}/status", c.handleUpdateJobStatus))

	return nil
}

// handleHealth is the handler for the health endpoint.
func (c *Controller) handleHealth(w http.ResponseWriter, r *http.Request) {
	response.Respond(w,
		response.WithString("OK"),
	)
}

// handleSubmitJob handles job submission from clients
func (c *Controller) handleSubmitJob(w http.ResponseWriter, r *http.Request) {
	var job Job
	if err := json.NewDecoder(r.Body).Decode(&job); err != nil {
		c.logger.Error("Failed to decode job", "error", err)
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	// Validate required fields
	if job.ID == "" {
		job.ID = fmt.Sprintf("job-%d", time.Now().Unix())
	}
	if job.Type == "" {
		job.Type = "download"
	}
	if job.Handler == "" {
		job.Handler = "hf"
	}
	if job.Config.ProviderID == "" {
		job.Config.ProviderID = "hf-public"
	}
	if job.Config.Revision == "" {
		job.Config.Revision = "main"
	}

	// Basic validation
	if job.Config.Repo == "" {
		http.Error(w, "repo is required", http.StatusBadRequest)
		return
	}
	if len(job.Config.Files) == 0 {
		http.Error(w, "files list cannot be empty", http.StatusBadRequest)
		return
	}
	if job.Config.LocalPath == "" {
		http.Error(w, "local_path is required", http.StatusBadRequest)
		return
	}

	if err := c.SubmitJob(&job); err != nil {
		c.logger.Error("Failed to submit job", "error", err)
		http.Error(w, "Failed to submit job", http.StatusInternalServerError)
		return
	}

	var payload response.JSON = make(response.JSON)
	payload["message"] = "Job submitted successfully"
	payload["job_id"] = job.ID
	payload["status"] = job.Status

	response.Respond(w,
		response.WithJSONStatus(payload, http.StatusCreated),
	)
}

// handleListJobs returns all jobs
func (c *Controller) handleListJobs(w http.ResponseWriter, r *http.Request) {
	jobs := c.ListJobs()

	var payload response.JSON = make(response.JSON)
	payload["jobs"] = jobs
	payload["count"] = len(jobs)

	response.Respond(w,
		response.WithJSON(payload),
	)
}

// handleGetJob returns a specific job by ID using path parameter
func (c *Controller) handleGetJob(w http.ResponseWriter, r *http.Request) {
	// Use Go 1.22+ path parameter extraction
	jobID := r.PathValue("id")
	if jobID == "" {
		http.Error(w, "Job ID is required", http.StatusBadRequest)
		return
	}

	job, exists := c.GetJob(jobID)
	if !exists {
		http.Error(w, "Job not found", http.StatusNotFound)
		return
	}

	var payload response.JSON = make(response.JSON)
	payload["job"] = job

	response.Respond(w,
		response.WithJSON(payload),
	)
}

// handleGetNextJob returns the next job for an agent to process
func (c *Controller) handleGetNextJob(w http.ResponseWriter, r *http.Request) {
	// Set a timeout for waiting for jobs
	ctx := r.Context()
	job, err := c.GetNextJob(ctx)
	if err != nil {
		if err == ctx.Err() {
			http.Error(w, "Request timeout", http.StatusRequestTimeout)
		} else {
			c.logger.Error("Failed to get next job", "error", err)
			http.Error(w, "Failed to get next job", http.StatusInternalServerError)
		}
		return
	}

	var payload response.JSON = make(response.JSON)
	payload["job"] = job

	response.Respond(w,
		response.WithJSON(payload),
	)
}

// handleUpdateJobStatus updates job status from agents using path parameter
func (c *Controller) handleUpdateJobStatus(w http.ResponseWriter, r *http.Request) {
	// Use Go 1.22+ path parameter extraction
	jobID := r.PathValue("id")
	if jobID == "" {
		http.Error(w, "Job ID is required", http.StatusBadRequest)
		return
	}

	var statusUpdate struct {
		Status string `json:"status"`
		Error  string `json:"error,omitempty"`
	}

	if err := json.NewDecoder(r.Body).Decode(&statusUpdate); err != nil {
		c.logger.Error("Failed to decode status update", "error", err)
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	if err := c.UpdateJobStatus(jobID, statusUpdate.Status, statusUpdate.Error); err != nil {
		c.logger.Error("Failed to update job status", "error", err)
		http.Error(w, "Failed to update job status", http.StatusInternalServerError)
		return
	}

	var payload response.JSON = make(response.JSON)
	payload["message"] = "Job status updated successfully"
	payload["job_id"] = jobID
	payload["status"] = statusUpdate.Status

	response.Respond(w,
		response.WithJSON(payload),
	)
}
