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
	"syncbit/internal/core/types"
	"syncbit/internal/provider"
	"syncbit/internal/runner"
	"syncbit/internal/transport"
)

const (
	// Version represents the current agent version
	Version = "0.1.0"
)

// RegisterHandlers registers the handlers for the agent.
func (a *Agent) RegisterHandlers(registrar api.HandlerRegistrar) error {
	routes := []api.Route{
		api.NewRoute(api.MethodGet, "/health", a.handleHealth),
		api.NewRoute(api.MethodGet, "/state", a.handleGetState),
		api.NewRoute(api.MethodGet, "/info", a.handleGetInfo),
		api.NewRoute(api.MethodGet, "/cache/stats", a.handleGetCacheStats),
		api.NewRoute(api.MethodGet, "/files/availability", a.handleGetFileAvailability),
		api.NewRoute(api.MethodPost, "/jobs", a.handleSubmitJob),
		api.NewRoute(api.MethodGet, "/jobs/{id}", a.handleGetJob),
		api.NewRoute(api.MethodDelete, "/jobs/{id}", a.handleCancelJob),
		api.NewRoute(api.MethodGet, "/datasets", a.handleListDatasets),
		api.NewRoute(api.MethodGet, "/datasets/{name}/files", a.handleListFiles),
		api.NewRoute(api.MethodGet, "/datasets/{name}/file-content/{path...}", a.handleGetFile),
		api.NewRoute(api.MethodGet, "/datasets/{name}/file-info/{path...}", a.handleGetFileInfo),
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
	payload["version"] = Version
	payload["listen_addr"] = a.cfg.Network.ListenAddr.URL()
	payload["advertise_addr"] = a.cfg.Network.AdvertiseAddr.URL()

	response.Respond(w, response.WithJSON(payload))
}

// handleGetCacheStats returns cache statistics
func (a *Agent) handleGetCacheStats(w http.ResponseWriter, r *http.Request) {
	stats := a.cache.GetCacheStats()

	var payload response.JSON = make(response.JSON)
	payload["cache_stats"] = stats

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

	if job.Config.FilePath == "" {
		a.log.Error("Job has no file to download", "job_id", job.ID)
		http.Error(w, "File path must be specified", http.StatusBadRequest)
		return
	}

	if job.Config.ProviderSource.ProviderID == "" {
		a.log.Error("Job missing provider source", "job_id", job.ID, "handler", job.Handler)
		http.Error(w, "Job must specify a provider source", http.StatusBadRequest)
		return
	}

	a.log.Info("Received job from controller",
		"job_id", job.ID,
		"handler", job.Handler,
		"file", job.Config.FilePath,
		"provider", job.Config.ProviderSource.ProviderID,
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
	job.SetStatus(types.StatusPending, "")

	// Store the job in our jobs map
	a.jobs[job.ID] = &job
	a.jobMutex.Unlock()

	// Create a runner job based on the job type and handler
	var runnerJob *runner.Job
	switch job.Handler {
	case types.JobHandlerDownload:
		runnerJob = a.createDownloadJob(&job)
	default:
		// For backward compatibility, treat all job types as download jobs
		runnerJob = a.createDownloadJob(&job)
	}

	// Submit job to pool
	if err := a.pool.Submit(runnerJob); err != nil {
		a.log.Error("Failed to submit job to pool", "job_id", job.ID, "error", err)

		a.jobMutex.Lock()
		job.SetStatus(types.StatusFailed, "failed to queue job")
		a.jobMutex.Unlock()

		http.Error(w, "Failed to queue job", http.StatusInternalServerError)
		return
	}

	// Store the job context cancellation function
	jobCtx, cancelFunc := context.WithCancel(context.Background())
	a.jobMutex.Lock()
	a.jobCancelFuncs[job.ID] = cancelFunc
	job.SetStatus(types.StatusRunning, "")
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
	payload["status"] = string(types.StatusRunning)

	response.Respond(w, response.WithJSONStatus(payload, http.StatusAccepted))
}

// createDownloadJob creates a unified runner job for downloading a single file from a single provider
func (a *Agent) createDownloadJob(job *types.Job) *runner.Job {
	return runner.NewJob(job.ID, func(ctx context.Context, rjob *runner.Job) error {
		a.log.Info("Starting download job",
			"job_id", job.ID,
			"file", job.Config.FilePath,
			"provider", job.Config.ProviderSource.ProviderID,
		)

		// Validate job config
		if job.Config.FilePath == "" {
			return fmt.Errorf("no file path specified")
		}
		if job.Config.ProviderSource.ProviderID == "" {
			return fmt.Errorf("no provider source specified")
		}

		// Create dataset name from repo/revision for cache
		datasetName := fmt.Sprintf("%s-%s", job.Config.Repo, job.Config.Revision)

		// Download from the single specified provider
		err := a.downloadFromProviderSource(ctx, rjob, job, job.Config.ProviderSource, datasetName, job.Config.FilePath)
		if err != nil {
			return fmt.Errorf("download failed from provider %s: %w", job.Config.ProviderSource.ProviderID, err)
		}

		a.log.Info("File downloaded successfully",
			"job_id", job.ID,
			"file", job.Config.FilePath,
			"provider", job.Config.ProviderSource.ProviderID,
		)
		return nil
	})
}

// downloadFromProviderSource downloads a file from a specific provider source
func (a *Agent) downloadFromProviderSource(ctx context.Context, rjob *runner.Job, job *types.Job, providerSource types.ProviderSource, datasetName, filePath string) error {
	// Get the provider
	prov, err := provider.GetProvider(providerSource.ProviderID)
	if err != nil {
		return fmt.Errorf("failed to get provider %s: %w", providerSource.ProviderID, err)
	}

	// Handle peer providers differently (they need the peer address)
	if providerSource.ProviderID == "peer-main" && (providerSource.PeerAddr != types.Address{}) {
		return a.downloadFromPeer(ctx, rjob, prov, providerSource.PeerAddr, datasetName, filePath)
	}

	// Handle upstream providers (HF, S3, HTTP, etc.)
	return a.downloadFromUpstreamProvider(ctx, rjob, prov, job, datasetName, filePath)
}

// downloadFromPeer downloads a file from a peer agent
func (a *Agent) downloadFromPeer(ctx context.Context, rjob *runner.Job, prov provider.Provider, peerAddr types.Address, datasetName, filePath string) error {
	peerProv, ok := prov.(*provider.PeerProvider)
	if !ok {
		return fmt.Errorf("provider is not a peer provider")
	}

	// Check if peer has the file
	hasFile, err := peerProv.GetFileAvailabilityFromPeer(ctx, peerAddr, datasetName, filePath)
	if err != nil {
		return fmt.Errorf("failed to check file availability on peer: %w", err)
	}
	if !hasFile {
		return fmt.Errorf("peer does not have file %s", filePath)
	}

	// Download the file from peer
	fileURL := fmt.Sprintf("%s/datasets/%s/files/%s", peerAddr.URL(), datasetName, filePath)

	httpTransfer := transport.NewHTTPTransfer(
		transport.HTTPWithClient(transport.DefaultHTTPClient()),
	)

	var reqOpts []transport.HTTPRequestOption
	if headers := peerProv.GetHeaders(); len(headers) > 0 {
		reqOpts = append(reqOpts, transport.HTTPRequestHeaders(headers))
	}

	err = httpTransfer.Get(ctx, fileURL, func(resp *http.Response) error {
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("peer returned status %d", resp.StatusCode)
		}

		return a.storeStreamInCache(ctx, rjob, datasetName, filePath, resp.Body, types.Bytes(resp.ContentLength))
	}, reqOpts...)

	if err != nil {
		return fmt.Errorf("failed to download from peer: %w", err)
	}

	return nil
}

// downloadFromUpstreamProvider downloads a file from an upstream provider (HF, S3, HTTP, etc.)
func (a *Agent) downloadFromUpstreamProvider(ctx context.Context, rjob *runner.Job, prov provider.Provider, job *types.Job, datasetName, filePath string) error {
	// Handle different provider types
	switch p := prov.(type) {
	case *provider.HFProvider:
		return a.downloadFromHFProvider(ctx, rjob, p, job, datasetName, filePath)
	default:
		return fmt.Errorf("unsupported provider type for upstream download: %T", prov)
	}
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
	if job.Status != types.StatusRunning && job.Status != types.StatusPending {
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
	job.SetStatus(types.StatusCancelled, "Job cancelled by request")

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
	payload["status"] = string(types.StatusCancelled)

	response.Respond(w,
		response.WithJSON(payload),
	)
}

// handleListDatasets lists available datasets on this agent
func (a *Agent) handleListDatasets(w http.ResponseWriter, r *http.Request) {
	datasets := a.cache.ListDatasets()

	datasetList := make([]interface{}, len(datasets))
	for i, name := range datasets {
		datasetList[i] = map[string]interface{}{
			"name": name,
		}
	}

	var payload response.JSON = make(response.JSON)
	payload["datasets"] = datasetList
	payload["count"] = len(datasets)

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

	files := a.cache.ListFiles(datasetName)

	fileList := make([]interface{}, len(files))
	for i, path := range files {
		fileList[i] = map[string]interface{}{
			"path": path,
		}
	}

	var payload response.JSON = make(response.JSON)
	payload["dataset"] = datasetName
	payload["files"] = fileList
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

	a.log.Debug("File requested", "dataset", datasetName, "file", filePath)

	// Check if file exists in cache
	if !a.cache.HasFileByPath(datasetName, filePath) {
		http.Error(w, "File not found", http.StatusNotFound)
		return
	}

	// Get file size for the reader
	data, err := a.cache.GetFileByPath(datasetName, filePath)
	if err != nil {
		a.log.Error("Failed to get file from cache", "error", err)
		http.Error(w, "Failed to read file", http.StatusInternalServerError)
		return
	}
	fileSize := types.Bytes(len(data))

	// Create a cache reader with rate limiting
	rw := a.cache.NewCacheReader(datasetName, filePath, fileSize)
	reader := rw.Reader(r.Context())
	defer func() {
		if closer, ok := reader.(io.Closer); ok {
			closer.Close()
		}
	}()

	// Serve the file by streaming from cache
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", fmt.Sprintf("%d", fileSize))
	w.WriteHeader(http.StatusOK)

	_, err = io.Copy(w, reader)
	if err != nil {
		a.log.Error("Failed to stream file to client", "error", err)
	}
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
	if !a.cache.HasFileByPath(dataset, filePath) {
		http.Error(w, "File not found", http.StatusNotFound)
		return
	}

	// Get file data to determine size
	data, err := a.cache.GetFileByPath(dataset, filePath)
	if err != nil {
		a.log.Error("Failed to get file from cache", "error", err)
		http.Error(w, "Failed to read file", http.StatusInternalServerError)
		return
	}

	// Create response with file information
	response := map[string]interface{}{
		"file": map[string]interface{}{
			"path": filePath,
			"size": len(data),
		},
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
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

// storeStreamInCache delegates stream storage to the cache system
func (a *Agent) storeStreamInCache(ctx context.Context, rjob *runner.Job, datasetName, filePath string, reader io.Reader, fileSize types.Bytes) error {
	// Create a cache-backed writer with progress tracking
	rw := a.cache.NewCacheWriter(datasetName, filePath, fileSize,
		types.RWWithIOReader(reader),
		types.RWWithReaderCallback(func(n int64) {
			rjob.Tracker().IncCurrent(n)
		}),
	)

	// Transfer data from reader to cache writer
	_, err := rw.Transfer(ctx)
	if err != nil {
		return fmt.Errorf("failed to transfer data to cache: %w", err)
	}

	// Close the writer to ensure write-through to disk
	return rw.CloseWriter()
}

// handleGetFileAvailability returns information about what files this agent has available
func (a *Agent) handleGetFileAvailability(w http.ResponseWriter, r *http.Request) {
	availability := a.cache.GetAgentFileAvailability()

	var payload response.JSON = make(response.JSON)
	payload["availability"] = availability

	response.Respond(w, response.WithJSON(payload))
}
