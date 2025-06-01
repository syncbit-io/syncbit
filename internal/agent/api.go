package agent

import (
	"encoding/json"
	"fmt"
	"net/http"

	"syncbit/internal/api"
	"syncbit/internal/api/response"
	"syncbit/internal/core/types"
)

const (
	// Version represents the current agent version
	Version = "0.1.0"
)

// RegisterHandlers registers the handlers for the agent.
func (a *Agent) RegisterHandlers(registrar api.HandlerRegistrar) error {
	routes := []api.Route{
		api.NewRoute(http.MethodGet, "/health", a.handleHealth),
		api.NewRoute(http.MethodGet, "/state", a.handleGetState),
		api.NewRoute(http.MethodGet, "/info", a.handleGetInfo),
		api.NewRoute(http.MethodGet, "/cache/stats", a.handleGetCacheStats),
		api.NewRoute(http.MethodGet, "/files/availability", a.handleGetFileAvailability),
		api.NewRoute(http.MethodGet, "/assignments", a.handleListAssignments),
		api.NewRoute(http.MethodGet, "/assignments/{id}", a.handleGetAssignment),
		api.NewRoute(http.MethodGet, "/datasets", a.handleListDatasets),
		api.NewRoute(http.MethodGet, "/datasets/{name}/files", a.handleListFiles),
		api.NewRoute(http.MethodGet, "/datasets/{name}/file-content/{path...}", a.handleGetFile),
		api.NewRoute(http.MethodGet, "/datasets/{name}/file-info/{path...}", a.handleGetFileInfo),
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
	state := a.buildAgentState()

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
	payload["listen_addr"] = a.cfg.ListenAddr
	payload["advertise_addr"] = a.cfg.AdvertiseAddr

	response.Respond(w, response.WithJSON(payload))
}

// handleGetCacheStats returns cache statistics
func (a *Agent) handleGetCacheStats(w http.ResponseWriter, r *http.Request) {
	stats := a.getCacheStats()

	var payload response.JSON = make(response.JSON)
	payload["cache_stats"] = stats

	response.Respond(w, response.WithJSON(payload))
}

// handleListAssignments returns all current dataset assignments
func (a *Agent) handleListAssignments(w http.ResponseWriter, r *http.Request) {
	a.assignmentsMu.RLock()
	assignments := make([]*types.DatasetAssignment, 0, len(a.assignments))
	for _, assignment := range a.assignments {
		assignments = append(assignments, assignment)
	}
	a.assignmentsMu.RUnlock()

	var payload response.JSON = make(response.JSON)
	payload["assignments"] = assignments
	payload["count"] = len(assignments)

	response.Respond(w, response.WithJSON(payload))
}

// handleGetAssignment returns a specific dataset assignment by ID
func (a *Agent) handleGetAssignment(w http.ResponseWriter, r *http.Request) {
	assignmentID := r.PathValue("id")
	if assignmentID == "" {
		http.Error(w, "Assignment ID is required", http.StatusBadRequest)
		return
	}

	a.assignmentsMu.RLock()
	assignment, exists := a.assignments[assignmentID]
	a.assignmentsMu.RUnlock()

	if !exists {
		http.Error(w, "Assignment not found", http.StatusNotFound)
		return
	}

	var payload response.JSON = make(response.JSON)
	payload["assignment"] = assignment

	response.Respond(w, response.WithJSON(payload))
}

// handleListDatasets lists available datasets on this agent
func (a *Agent) handleListDatasets(w http.ResponseWriter, r *http.Request) {
	datasets := a.listDatasets()

	datasetList := make([]any, len(datasets))
	for i, name := range datasets {
		datasetList[i] = map[string]any{
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

	files := a.listFiles()

	fileList := make([]any, 0, len(files))
	for path, fileInfo := range files {
		fileList = append(fileList, map[string]any{
			"path": path,
			"size": fileInfo.Size,
		})
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

	a.logger.Debug("File requested", "dataset", datasetName, "file", filePath)

	// Check if file exists in cache
	if !a.hasFileByPath(datasetName, filePath) {
		http.Error(w, "File not found", http.StatusNotFound)
		return
	}

	// Get file size for the reader
	data, err := a.getFileByPath(datasetName, filePath)
	if err != nil {
		a.logger.Error("Failed to get file from cache", "error", err)
		http.Error(w, "Failed to read file", http.StatusInternalServerError)
		return
	}
	fileSize := types.Bytes(len(data))

	// For now, just return the data directly
	// TODO: Implement streaming from block cache reader

	// Serve the file by streaming from cache
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", fmt.Sprintf("%d", fileSize))
	w.WriteHeader(http.StatusOK)

	_, err = w.Write(data)
	if err != nil {
		a.logger.Error("Failed to write file to client", "error", err)
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
	if !a.hasFileByPath(dataset, filePath) {
		http.Error(w, "File not found", http.StatusNotFound)
		return
	}

	// Get file data to determine size
	data, err := a.getFileByPath(dataset, filePath)
	if err != nil {
		a.logger.Error("Failed to get file from cache", "error", err)
		http.Error(w, "Failed to read file", http.StatusInternalServerError)
		return
	}

	// Create response with file information
	response := map[string]any{
		"file": map[string]any{
			"path": filePath,
			"size": len(data),
		},
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

// handleGetFileAvailability returns information about what files this agent has available
func (a *Agent) handleGetFileAvailability(w http.ResponseWriter, r *http.Request) {
	// For now, return basic availability info
	// TODO: Get actual file availability from block cache
	availability := map[string]any{"available": true}

	var payload response.JSON = make(response.JSON)
	payload["availability"] = availability

	response.Respond(w, response.WithJSON(payload))
}