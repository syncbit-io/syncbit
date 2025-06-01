package controller

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"syncbit/internal/api"
	"syncbit/internal/api/response"
	"syncbit/internal/core/types"
)

func (c *Controller) RegisterHandlers(registrar api.HandlerRegistrar) error {
	routes := []api.Route{
		api.NewRoute(http.MethodGet, "/health", c.handleHealth),
		
		api.NewRoute(http.MethodPost, "/datasets", c.handleCreateDataset),
		api.NewRoute(http.MethodGet, "/datasets", c.handleListDatasets),
		api.NewRoute(http.MethodGet, "/datasets/{name}/{revision}", c.handleGetDataset),
		api.NewRoute(http.MethodDelete, "/datasets/{name}/{revision}", c.handleDeleteDataset),
		
		api.NewRoute(http.MethodGet, "/agents", c.handleListAgents),
		api.NewRoute(http.MethodPost, "/agents/{id}/heartbeat", c.handleAgentHeartbeat),
		api.NewRoute(http.MethodGet, "/agents/{id}/assignments", c.handleGetAgentAssignments),
		
		api.NewRoute(http.MethodGet, "/files/{dataset}/{revision}/{path}/peers", c.handleGetFilePeers),
		api.NewRoute(http.MethodGet, "/files/{dataset}/{revision}/{path}/blocks/{blockIndex}/peers", c.handleGetBlockPeers),
	}

	for _, route := range routes {
		if err := registrar.RegisterHandler(route); err != nil {
			return err
		}
	}
	
	return nil
}

func (c *Controller) handleHealth(w http.ResponseWriter, r *http.Request) {
	response.Respond(w,
		response.WithJSONStatus(response.JSON{
			"status":      "healthy",
			"datasets":    len(c.datasets),
			"agents":      len(c.agents),
			"assignments": len(c.assignments),
		}, http.StatusOK))
}

func (c *Controller) handleCreateDataset(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name        string                   `json:"name"`
		Source      string                   `json:"source,omitempty"`
		Revision    string                   `json:"revision"`
		Replication int                      `json:"replication"`
		Priority    int                      `json:"priority"`
		Sources     []types.ProviderConfig   `json:"sources"`
		Files       []types.DatasetFile      `json:"files,omitempty"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		response.Respond(w, response.WithStringStatus("Invalid request body", http.StatusBadRequest))
		return
	}

	if req.Name == "" || req.Revision == "" {
		response.Respond(w, response.WithStringStatus("Name and revision are required", http.StatusBadRequest))
		return
	}

	if strings.Contains(req.Name, "/") {
		response.Respond(w, response.WithStringStatus("Dataset names cannot contain forward slashes", http.StatusBadRequest))
		return
	}

	if req.Replication < 1 {
		req.Replication = 1
	}

	dataset := types.NewDataset(req.Name, req.Revision, req.Replication)
	dataset.Source = req.Source
	dataset.Priority = req.Priority
	dataset.Sources = req.Sources
	dataset.Files = req.Files

	if err := c.CreateDataset(dataset); err != nil {
		if strings.Contains(err.Error(), "already exists") {
			response.Respond(w, response.WithStringStatus(err.Error(), http.StatusConflict))
		} else {
			response.Respond(w, response.WithStringStatus(err.Error(), http.StatusInternalServerError))
		}
		return
	}

	response.Respond(w, response.WithJSONStatus(response.JSON{"dataset": dataset}, http.StatusCreated))
}

func (c *Controller) handleListDatasets(w http.ResponseWriter, r *http.Request) {
	c.datasetsMu.RLock()
	datasets := make([]*types.Dataset, 0, len(c.datasets))
	for _, d := range c.datasets {
		datasets = append(datasets, d)
	}
	c.datasetsMu.RUnlock()

	response.Respond(w, response.WithJSONStatus(response.JSON{"datasets": datasets}, http.StatusOK))
}

func (c *Controller) handleGetDataset(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	revision := r.PathValue("revision")

	dataset, err := c.GetDataset(name, revision)
	if err != nil {
		response.Respond(w, response.WithStringStatus(err.Error(), http.StatusNotFound))
		return
	}

	response.Respond(w, response.WithJSONStatus(response.JSON{"dataset": dataset}, http.StatusOK))
}

func (c *Controller) handleDeleteDataset(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	revision := r.PathValue("revision")

	if err := c.DeleteDataset(name, revision); err != nil {
		if strings.Contains(err.Error(), "not found") {
			response.Respond(w, response.WithStringStatus(err.Error(), http.StatusNotFound))
		} else {
			response.Respond(w, response.WithStringStatus(err.Error(), http.StatusInternalServerError))
		}
		return
	}

	response.Respond(w, response.WithJSONStatus(response.JSON{
		"message": "Dataset deleted",
		"dataset": fmt.Sprintf("%s@%s", name, revision),
	}, http.StatusOK))
}

func (c *Controller) handleListAgents(w http.ResponseWriter, r *http.Request) {
	c.agentsMu.RLock()
	agents := make([]*types.Agent, 0, len(c.agents))
	for _, a := range c.agents {
		agents = append(agents, a)
	}
	c.agentsMu.RUnlock()

	response.Respond(w, response.WithJSONStatus(response.JSON{"agents": agents}, http.StatusOK))
}

func (c *Controller) handleAgentHeartbeat(w http.ResponseWriter, r *http.Request) {
	agentID := r.PathValue("id")

	var state types.AgentState
	if err := json.NewDecoder(r.Body).Decode(&state); err != nil {
		response.Respond(w, response.WithStringStatus("Invalid request body", http.StatusBadRequest))
		return
	}

	c.agentsMu.Lock()
	agent, exists := c.agents[agentID]
	if !exists {
		agent = &types.Agent{
			ID: agentID,
		}
		
		advertiseAddr := r.Header.Get("X-Advertise-Addr")
		if advertiseAddr != "" {
			if parsed, err := url.Parse(advertiseAddr); err == nil {
				agent.AdvertiseAddr = parsed
			}
		}
		
		c.agents[agentID] = agent
	}
	c.agentsMu.Unlock()

	if err := c.UpdateAgentState(agentID, state); err != nil {
		response.Respond(w, response.WithStringStatus(err.Error(), http.StatusInternalServerError))
		return
	}

	assignments, _ := c.GetAgentAssignments(agentID)

	response.Respond(w, response.WithJSONStatus(response.JSON{
		"agent_id": agentID,
		"assignments": assignments,
	}, http.StatusOK))
}

func (c *Controller) handleGetAgentAssignments(w http.ResponseWriter, r *http.Request) {
	agentID := r.PathValue("id")

	assignments, err := c.GetAgentAssignments(agentID)
	if err != nil {
		response.Respond(w, response.WithStringStatus(err.Error(), http.StatusInternalServerError))
		return
	}

	response.Respond(w, response.WithJSONStatus(response.JSON{"assignments": assignments}, http.StatusOK))
}

func (c *Controller) handleGetFilePeers(w http.ResponseWriter, r *http.Request) {
	dataset := r.PathValue("dataset")
	revision := r.PathValue("revision")
	filePath := r.PathValue("path")

	agents, err := c.GetAgentsWithFile(dataset, revision, filePath)
	if err != nil {
		response.Respond(w, response.WithStringStatus(err.Error(), http.StatusInternalServerError))
		return
	}

	peers := make([]map[string]any, 0, len(agents))
	for _, agent := range agents {
		peers = append(peers, map[string]any{
			"agent_id": agent.ID,
			"address": agent.AdvertiseAddr.String(),
			"last_seen": agent.LastHeartbeat,
		})
	}

	response.Respond(w, response.WithJSONStatus(response.JSON{
		"dataset": fmt.Sprintf("%s@%s", dataset, revision),
		"file": filePath,
		"peers": peers,
	}, http.StatusOK))
}

func (c *Controller) handleGetBlockPeers(w http.ResponseWriter, r *http.Request) {
	dataset := r.PathValue("dataset")
	revision := r.PathValue("revision")
	filePath := r.PathValue("path")
	blockIndexStr := r.PathValue("blockIndex")
	
	blockIndex, err := strconv.Atoi(blockIndexStr)
	if err != nil {
		response.Respond(w, response.WithStringStatus("Invalid block index", http.StatusBadRequest))
		return
	}

	agents, err := c.GetAgentsWithBlock(dataset, revision, filePath, blockIndex)
	if err != nil {
		response.Respond(w, response.WithStringStatus(err.Error(), http.StatusInternalServerError))
		return
	}

	peers := make([]map[string]any, 0, len(agents))
	for _, agent := range agents {
		peers = append(peers, map[string]any{
			"agent_id": agent.ID,
			"address": agent.AdvertiseAddr.String(),
			"last_seen": agent.LastHeartbeat,
		})
	}

	response.Respond(w, response.WithJSONStatus(response.JSON{
		"dataset": fmt.Sprintf("%s@%s", dataset, revision),
		"file": filePath,
		"block_index": blockIndex,
		"peers": peers,
	}, http.StatusOK))
}