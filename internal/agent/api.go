package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"syncbit/internal/api"
	"syncbit/internal/api/response"
	"syncbit/internal/core/tracker"
	"syncbit/internal/core/types"
	"syncbit/internal/provider"
	"syncbit/internal/runner"
	"syncbit/internal/transport"
)

// RegisterHandlers registers the handlers for the agent.
func (a *Agent) RegisterHandlers(registrar api.HandlerRegistrar) error {
	routes := []api.Route{
		api.NewRoute(api.MethodGet, "/health", a.handleHealth),
		api.NewRoute(api.MethodGet, "/state", a.handleGetState),
		api.NewRoute(api.MethodGet, "/info", a.handleGetInfo),
		api.NewRoute(api.MethodPost, "/jobs", a.handleSubmitJob),
		api.NewRoute(api.MethodGet, "/jobs/{id}", a.handleGetJob),
		api.NewRoute(api.MethodDelete, "/jobs/{id}", a.handleCancelJob),
		api.NewRoute(api.MethodGet, "/datasets", a.handleListDatasets),
		api.NewRoute(api.MethodGet, "/datasets/{name}/files", a.handleListFiles),
		api.NewRoute(api.MethodGet, "/datasets/{name}/files/{path...}", a.handleGetFile),
		api.NewRoute(api.MethodGet, "/datasets/{name}/files/{path...}/info", a.handleGetFileInfo),
	}

	for _, route := range routes {
		if err := registrar.RegisterHandler(route); err != nil {
			return err
		}
	}

	return nil
}

// handleHealth is the handler for the health endpoint.
func (a *Agent) handleHealth(w http.ResponseWriter, r *http.Request) {
	var payload response.JSON = make(response.JSON)
	payload["status"] = "healthy"
	response.Respond(w, response.WithJSON(payload))
}

// handleGetState returns the current agent state
func (a *Agent) handleGetState(w http.ResponseWriter, r *http.Request) {
	// Get cache stats for disk usage information
	cacheStats := a.cache.GetCacheStats()

	// Collect current active jobs
	a.jobMutex.RLock()
	activeJobs := make([]string, 0)
	for jobID, job := range a.jobs {
		if job.IsActive() {
			activeJobs = append(activeJobs, jobID)
		}
	}
	a.jobMutex.RUnlock()

	state := types.AgentState{
		DiskUsed:      types.Bytes(cacheStats.DiskUsage),
		DiskAvailable: types.Bytes(cacheStats.DiskLimit - cacheStats.DiskUsage),
		ActiveJobs:    activeJobs,
		LastUpdated:   time.Now(),
	}

	var payload response.JSON = make(response.JSON)
	payload["agent_id"] = a.cfg.ID
	payload["state"] = state

	response.Respond(w, response.WithJSON(payload))
}

// handleGetInfo returns agent information
func (a *Agent) handleGetInfo(w http.ResponseWriter, r *http.Request) {
	var payload response.JSON = make(response.JSON)
	payload["agent_id"] = a.cfg.ID
	payload["version"] = "dev" // TODO: Add version info
	payload["listen_addr"] = a.cfg.Network.ListenAddr.URL()
	payload["advertise_addr"] = a.cfg.Network.AdvertiseAddr.URL()

	response.Respond(w, response.WithJSON(payload))
}

// handleSubmitJob handles job submission from controllers
func (a *Agent) handleSubmitJob(w http.ResponseWriter, r *http.Request) {
	var job types.Job
	if err := json.NewDecoder(r.Body).Decode(&job); err != nil {
		a.log.Error("Failed to decode job", "error", err)
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	// Validate job parameters
	if job.ID == "" {
		a.log.Error("Job missing required ID")
		http.Error(w, "Job ID is required", http.StatusBadRequest)
		return
	}

	if job.Handler == "" {
		a.log.Error("Job missing required handler", "job_id", job.ID)
		http.Error(w, "Job handler is required", http.StatusBadRequest)
		return
	}

	if job.Config.ProviderID == "" && job.Handler != types.JobHandlerP2P {
		a.log.Error("Job missing provider ID", "job_id", job.ID, "handler", job.Handler)
		http.Error(w, "Provider ID is required for non-P2P jobs", http.StatusBadRequest)
		return
	}

	if len(job.Config.Files) == 0 {
		a.log.Error("Job has no files to download", "job_id", job.ID)
		http.Error(w, "At least one file must be specified", http.StatusBadRequest)
		return
	}

	a.log.Info("Received job from controller",
		"job_id", job.ID,
		"handler", job.Handler,
		"provider", job.Config.ProviderID,
		"repo", job.Config.Repo,
		"files_count", len(job.Config.Files),
	)

	// Check if job already exists
	a.jobMutex.Lock()
	if existingJob, exists := a.jobs[job.ID]; exists {
		a.jobMutex.Unlock()
		a.log.Warn("Job already exists", "job_id", job.ID, "current_status", existingJob.Status)

		var payload response.JSON = make(response.JSON)
		payload["message"] = "Job already exists"
		payload["job_id"] = job.ID
		payload["status"] = string(existingJob.Status)

		response.Respond(w, response.WithJSONStatus(payload, http.StatusConflict))
		return
	}

	// Initialize job status
	job.SetStatus(types.JobStatusPending, "")

	// Store the job in our jobs map
	a.jobs[job.ID] = &job
	a.jobMutex.Unlock()

	// Create a runner job based on the job type and handler
	var runnerJob *runner.Job
	switch job.Handler {
	case types.JobHandlerHF:
		runnerJob = a.createHFDownloadJob(&job)
	case types.JobHandlerP2P:
		runnerJob = a.createP2PDownloadJob(&job)
	default:
		a.log.Error("Unsupported job handler", "handler", job.Handler, "job_id", job.ID)

		a.jobMutex.Lock()
		job.SetStatus(types.JobStatusFailed, fmt.Sprintf("unsupported handler: %s", job.Handler))
		a.jobMutex.Unlock()

		http.Error(w, "Unsupported job handler", http.StatusBadRequest)
		return
	}

	// Submit job to pool
	if err := a.pool.Submit(runnerJob); err != nil {
		a.log.Error("Failed to submit job to pool", "job_id", job.ID, "error", err)

		a.jobMutex.Lock()
		job.SetStatus(types.JobStatusFailed, "failed to queue job")
		a.jobMutex.Unlock()

		http.Error(w, "Failed to queue job", http.StatusInternalServerError)
		return
	}

	// Store the job context cancellation function
	jobCtx, cancelFunc := context.WithCancel(context.Background())
	a.jobMutex.Lock()
	a.jobCancelFuncs[job.ID] = cancelFunc
	job.SetStatus(types.JobStatusRunning, "")
	a.jobMutex.Unlock()

	// Start monitoring the job context
	go func() {
		<-jobCtx.Done()
		// Job context was cancelled, clean up
		a.jobMutex.Lock()
		delete(a.jobCancelFuncs, job.ID)
		a.jobMutex.Unlock()
	}()

	a.log.Info("Job submitted to pool", "job_id", job.ID)

	var payload response.JSON = make(response.JSON)
	payload["message"] = "Job accepted"
	payload["job_id"] = job.ID
	payload["status"] = string(types.JobStatusRunning)

	response.Respond(w, response.WithJSONStatus(payload, http.StatusAccepted))
}

// createHFDownloadJob creates a runner job for HuggingFace downloads
func (a *Agent) createHFDownloadJob(job *types.Job) *runner.Job {
	return runner.NewJob(job.ID, func(ctx context.Context, rjob *runner.Job) error {
		a.log.Info("Starting HF download job", "job_id", job.ID, "files", len(job.Config.Files))

		// Get the provider to calculate total size for progress tracking
		prov, err := provider.GetProvider(job.Config.ProviderID)
		if err != nil {
			return fmt.Errorf("failed to get provider %s: %w", job.Config.ProviderID, err)
		}

		hfProv, ok := prov.(*provider.HFProvider)
		if !ok {
			return fmt.Errorf("provider %s is not an HF provider", job.Config.ProviderID)
		}

		// Calculate total size for progress tracking
		var totalSize int64
		for _, file := range job.Config.Files {
			fileInfo, err := hfProv.GetFileInfo(ctx, job.Config.Repo, job.Config.Revision, file)
			if err != nil {
				a.log.Warn("Could not get file info for file", "file", file, "error", err)
				// Continue without size info - progress will be less accurate
				continue
			}
			if fileInfo != nil {
				totalSize += int64(fileInfo.Size)
			}
		}

		if totalSize > 0 {
			rjob.Tracker().SetTotal(totalSize)
			a.log.Debug("Set total download size", "job_id", job.ID, "total_bytes", totalSize)
		}

		// Download each file
		for i, file := range job.Config.Files {
			// Check for cancellation before processing each file
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			a.log.Debug("Downloading file", "job_id", job.ID, "file", file, "progress", fmt.Sprintf("%d/%d", i+1, len(job.Config.Files)))

			if err := a.downloadFileFromProvider(ctx, rjob, job, file); err != nil {
				return fmt.Errorf("failed to download file %s: %w", file, err)
			}

			a.log.Debug("File download completed", "job_id", job.ID, "file", file)
		}

		a.log.Info("HF download job completed", "job_id", job.ID, "files_downloaded", len(job.Config.Files))
		return nil
	})
}

// createP2PDownloadJob creates a runner job for P2P downloads with fallback
func (a *Agent) createP2PDownloadJob(job *types.Job) *runner.Job {
	return runner.NewJob(job.ID, func(ctx context.Context, rjob *runner.Job) error {
		a.log.Info("Starting P2P download job with fallback", "job_id", job.ID, "files", len(job.Config.Files))

		// For P2P jobs, try peer sources first, then fallback to provider
		for i, file := range job.Config.Files {
			// Check for cancellation
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			a.log.Debug("Processing file for P2P download", "job_id", job.ID, "file", file, "progress", fmt.Sprintf("%d/%d", i+1, len(job.Config.Files)))

			// First, try P2P download
			err := a.downloadFileWithP2P(ctx, rjob, job, file)
			if err == nil {
				a.log.Debug("File downloaded via P2P", "job_id", job.ID, "file", file)
				continue
			}

			// P2P failed, fallback to provider download
			a.log.Debug("P2P download failed, falling back to provider", "job_id", job.ID, "file", file, "p2p_error", err)

			if err := a.downloadFileFromProvider(ctx, rjob, job, file); err != nil {
				return fmt.Errorf("failed to download file %s via provider fallback: %w", file, err)
			}

			a.log.Debug("File downloaded via provider fallback", "job_id", job.ID, "file", file)
		}

		a.log.Info("P2P download job completed", "job_id", job.ID, "files_downloaded", len(job.Config.Files))
		return nil
	})
}

// handleGetJob returns job status
func (a *Agent) handleGetJob(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("id")
	if jobID == "" {
		http.Error(w, "Job ID is required", http.StatusBadRequest)
		return
	}

	// Look up job in our jobs map
	a.jobMutex.RLock()
	job, exists := a.jobs[jobID]
	a.jobMutex.RUnlock()

	if !exists {
		http.Error(w, "Job not found", http.StatusNotFound)
		return
	}

	var payload response.JSON = make(response.JSON)
	payload["job_id"] = job.ID
	payload["status"] = string(job.Status)
	payload["handler"] = job.Handler
	if job.Error != "" {
		payload["error"] = job.Error
	}

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

	a.jobMutex.Lock()
	job, exists := a.jobs[jobID]
	if !exists {
		a.jobMutex.Unlock()
		a.log.Warn("Job cancellation requested for non-existent job", "job_id", jobID)
		http.Error(w, "Job not found", http.StatusNotFound)
		return
	}

	// Check if job can be cancelled
	if job.Status != types.JobStatusRunning && job.Status != types.JobStatusPending {
		a.jobMutex.Unlock()
		a.log.Warn("Job cancellation requested for non-running job", "job_id", jobID, "status", job.Status)

		var payload response.JSON = make(response.JSON)
		payload["message"] = "Job cannot be cancelled"
		payload["job_id"] = jobID
		payload["status"] = string(job.Status)
		payload["reason"] = "Job is not running or pending"

		response.Respond(w, response.WithJSONStatus(payload, http.StatusConflict))
		return
	}

	// Get the cancel function
	cancelFunc, hasCancelFunc := a.jobCancelFuncs[jobID]

	// Update job status immediately
	job.SetStatus(types.JobStatusCancelled, "Job cancelled by request")

	a.jobMutex.Unlock()

	// Call the cancel function if available
	if hasCancelFunc {
		a.log.Info("Cancelling job", "job_id", jobID)
		cancelFunc()
	} else {
		a.log.Warn("Job cancellation requested but no cancel function available", "job_id", jobID)
	}

	// Report cancellation to controller
	go a.reportJobStatus(context.Background(), job)

	var payload response.JSON = make(response.JSON)
	payload["message"] = "Job cancellation initiated"
	payload["job_id"] = jobID
	payload["status"] = string(types.JobStatusCancelled)

	response.Respond(w,
		response.WithJSON(payload),
	)
}

// handleListDatasets lists available datasets on this agent
func (a *Agent) handleListDatasets(w http.ResponseWriter, r *http.Request) {
	// TODO: Implement dataset discovery from storage
	// For now, return empty list
	var payload response.JSON = make(response.JSON)
	payload["datasets"] = []interface{}{}
	payload["count"] = 0

	response.Respond(w,
		response.WithJSON(payload),
	)
}

// handleListFiles lists files in a specific dataset
func (a *Agent) handleListFiles(w http.ResponseWriter, r *http.Request) {
	datasetName := r.PathValue("name")
	if datasetName == "" {
		http.Error(w, "Dataset name is required", http.StatusBadRequest)
		return
	}

	// TODO: Implement file listing for dataset
	// For now, return empty list
	var payload response.JSON = make(response.JSON)
	payload["dataset"] = datasetName
	payload["files"] = []interface{}{}
	payload["count"] = 0

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

// handleGetFileInfo returns file information for a specific file
func (a *Agent) handleGetFileInfo(w http.ResponseWriter, r *http.Request) {
	dataset := r.PathValue("name")
	filePath := r.PathValue("path")

	if dataset == "" || filePath == "" {
		http.Error(w, "Dataset name and file path are required", http.StatusBadRequest)
		return
	}

	// Check if file exists in cache
	if a.cache.HasFileByPath(dataset, filePath) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"exists": true}`))
	} else {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`{"exists": false}`))
	}
}

// downloadFileFromProvider downloads a file using the configured provider
func (a *Agent) downloadFileFromProvider(ctx context.Context, rjob *runner.Job, job *types.Job, filePath string) error {
	// Get the provider
	prov, err := provider.GetProvider(job.Config.ProviderID)
	if err != nil {
		return fmt.Errorf("failed to get provider %s: %w", job.Config.ProviderID, err)
	}

	// Create dataset name from repo/revision for cache
	datasetName := fmt.Sprintf("%s-%s", job.Config.Repo, job.Config.Revision)

	// Handle different provider types - delegate download to provider
	switch prov.GetID() {
	default:
		// Try to cast to HFProvider for HuggingFace
		if hfProv, ok := prov.(*provider.HFProvider); ok {
			return a.downloadFromHFProvider(ctx, rjob, hfProv, job, datasetName, filePath)
		} else {
			return fmt.Errorf("unsupported provider type for download: %T", prov)
		}
	}
}

// downloadFromHFProvider delegates HuggingFace download to provider and cache
func (a *Agent) downloadFromHFProvider(ctx context.Context, rjob *runner.Job, hfProv *provider.HFProvider, job *types.Job, datasetName, filePath string) error {
	// Check for cancellation before starting
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Build download URL (metadata is already used in total calculation)
	downloadURL := hfProv.BuildDownloadURL(job.Config.Repo, job.Config.Revision, filePath)

	// Create HTTP transport for download
	httpTransfer := transport.NewHTTPTransfer(
		transport.HTTPWithClient(transport.DefaultHTTPClient()),
	)

	// Download directly to cache - let cache handle all file/block operations
	var reqOpts []transport.HTTPRequestOption
	if token := hfProv.GetToken(); token != "" {
		reqOpts = append(reqOpts, transport.HTTPRequestHeaders(map[string]string{
			"Authorization": fmt.Sprintf("Bearer %s", token),
		}))
	}

	err := httpTransfer.Get(ctx, downloadURL, func(resp *http.Response) error {
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("HTTP request failed with status %d", resp.StatusCode)
		}

		// Delegate to cache system to handle download and storage
		return a.storeStreamInCache(ctx, rjob, datasetName, filePath, resp.Body, types.Bytes(resp.ContentLength))
	}, reqOpts...)

	if err != nil {
		return fmt.Errorf("failed to download file: %w", err)
	}

	return nil
}

// downloadFileWithP2P attempts to download a file using peer-to-peer transfer with fallback
func (a *Agent) downloadFileWithP2P(ctx context.Context, rjob *runner.Job, job *types.Job, filePath string) error {
	// Check for cancellation
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	a.log.Debug("Attempting P2P download", "file", filePath, "job_id", job.ID)

	// TODO: In a complete implementation, this would:
	// 1. Query controller for peer sources for this file
	// 2. Check each peer to see if they have the file/blocks we need
	// 3. Download blocks from multiple peers in parallel
	// 4. Validate block integrity using checksums
	// 5. Delegate to cache system to fetch blocks from peers
	// 6. Cache system handles all block assembly and storage

	// For now, P2P is not fully implemented, so we'll return an error to trigger fallback
	// This is intentional - the caller expects this and will fallback to provider download
	a.log.Debug("P2P download not yet fully implemented, triggering fallback", "file", filePath)
	return fmt.Errorf("P2P download not yet implemented for file %s", filePath)
}

// storeStreamInCache delegates stream storage to the cache system
func (a *Agent) storeStreamInCache(ctx context.Context, rjob *runner.Job, datasetName, filePath string, reader io.Reader, fileSize types.Bytes) error {
	// Create a reader with progress tracking
	progressReader := &progressTrackingReader{
		reader:  reader,
		tracker: rjob.Tracker(),
	}

	// Use the cache system's StreamStore method for direct storage
	return a.cache.StreamStore(datasetName, filePath, progressReader, fileSize)
}

// progressTrackingReader wraps a reader to track progress
type progressTrackingReader struct {
	reader  io.Reader
	tracker *tracker.Tracker
}

func (ptr *progressTrackingReader) Read(p []byte) (int, error) {
	n, err := ptr.reader.Read(p)
	if n > 0 {
		ptr.tracker.IncCurrent(int64(n))
	}
	return n, err
}
