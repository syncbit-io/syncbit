package controller

import (
	"context"
	"fmt"
	"net/url"
	"sync"
	"time"

	"syncbit/internal/api"
	"syncbit/internal/config"
	"syncbit/internal/core/logger"
	"syncbit/internal/core/types"
)

type Controller struct {
	cfg    *types.ControllerConfig
	logger *logger.Logger
	server *api.Server

	datasets    map[string]*types.Dataset
	datasetsMu  sync.RWMutex

	assignments   map[string]*types.DatasetAssignment
	assignmentsMu sync.RWMutex

	agents   map[string]*types.Agent
	agentsMu sync.RWMutex

	scheduler *DatasetScheduler
	ctx       context.Context
	cancel    context.CancelFunc
}

func NewController(configFile string, debug bool) (*Controller, error) {
	cfg, err := config.LoadConfig(configFile)
	if err != nil {
		cfg = &types.Config{
			Debug:      debug,
			Controller: &types.ControllerConfig{},
		}
	}

	if debug {
		cfg.Debug = true
	}

	if cfg.Controller == nil {
		defaults := types.DefaultControllerConfig()
		cfg.Controller = &defaults
	}

	listenAddr := cfg.Controller.ListenAddr
	if listenAddr == nil {
		listenAddr, _ = url.Parse("http://0.0.0.0:8080")
	}

	ctx, cancel := context.WithCancel(context.Background())

	loggerOpts := []logger.LoggerOption{logger.WithName("controller")}
	if cfg.Debug {
		loggerOpts = append(loggerOpts, logger.WithLevel(logger.LevelDebug))
	}

	c := &Controller{
		cfg:         cfg.Controller,
		logger:      logger.NewLogger(loggerOpts...),
		server:      api.NewServer(api.WithListen(listenAddr)),
		datasets:    make(map[string]*types.Dataset),
		assignments: make(map[string]*types.DatasetAssignment),
		agents:      make(map[string]*types.Agent),
		ctx:         ctx,
		cancel:      cancel,
	}

	c.scheduler = NewDatasetScheduler(c)

	if err := c.RegisterHandlers(c.server); err != nil {
		return nil, fmt.Errorf("failed to register handlers: %w", err)
	}

	return c, nil
}

func (c *Controller) Start(ctx context.Context) error {
	c.logger.Info("Starting controller", "listen_addr", c.cfg.ListenAddr)

	go c.reconcileLoop(ctx)
	go c.cleanupLoop(ctx)
	go c.rebalanceLoop(ctx)

	return c.server.Run(ctx)
}

func (c *Controller) Stop() error {
	c.cancel()
	return nil
}

func (c *Controller) reconcileLoop(ctx context.Context) {
	syncInterval := types.ParseDuration(c.cfg.SyncInterval, 30*time.Second)
	ticker := time.NewTicker(syncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.reconcileDatasets()
		}
	}
}

func (c *Controller) cleanupLoop(ctx context.Context) {
	agentTimeout := types.ParseDuration(c.cfg.AgentTimeout, 5*time.Minute)
	ticker := time.NewTicker(agentTimeout)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.cleanupUnhealthyAgents()
		}
	}
}

func (c *Controller) rebalanceLoop(ctx context.Context) {
	ticker := time.NewTicker(15 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.rebalanceDatasets()
		}
	}
}

func (c *Controller) reconcileDatasets() {
	c.datasetsMu.RLock()
	datasets := make([]*types.Dataset, 0, len(c.datasets))
	for _, d := range c.datasets {
		datasets = append(datasets, d)
	}
	c.datasetsMu.RUnlock()

	for _, dataset := range datasets {
		c.reconcileDataset(dataset)
	}
}

func (c *Controller) reconcileDataset(dataset *types.Dataset) {
	readyReplicas := 0
	totalReplicas := 0

	c.assignmentsMu.RLock()
	for _, assignment := range c.assignments {
		if assignment.Dataset == dataset.Name && assignment.Revision == dataset.Revision {
			totalReplicas++
			if assignment.IsReady() {
				readyReplicas++
			}
		}
	}
	c.assignmentsMu.RUnlock()

	dataset.Status.ReadyReplicas = readyReplicas
	dataset.Status.TotalReplicas = totalReplicas
	dataset.Status.LastUpdated = time.Now()

	if readyReplicas < dataset.Replication {
		needed := dataset.Replication - totalReplicas
		if needed > 0 {
			dataset.Status.Phase = types.DatasetPhaseScheduling
			c.scheduler.ScheduleDataset(dataset, needed)
		} else {
			dataset.Status.Phase = types.DatasetPhaseDownloading
		}
	} else {
		dataset.Status.Phase = types.DatasetPhaseReady
		dataset.Status.Message = fmt.Sprintf("Dataset ready with %d/%d replicas", readyReplicas, dataset.Replication)
	}
}

func (c *Controller) cleanupUnhealthyAgents() {
	c.agentsMu.Lock()
	defer c.agentsMu.Unlock()

	agentTimeout := types.ParseDuration(c.cfg.AgentTimeout, 1*time.Minute)
	for id, agent := range c.agents {
		if !agent.IsHealthy(agentTimeout) {
			c.logger.Warn("Removing unhealthy agent", "agent_id", id)
			delete(c.agents, id)
			c.reassignDatasets(id)
		}
	}
}

func (c *Controller) reassignDatasets(agentID string) {
	c.assignmentsMu.Lock()
	defer c.assignmentsMu.Unlock()

	for id, assignment := range c.assignments {
		if assignment.AgentID == agentID {
			assignment.Status.Phase = types.AssignmentPhaseFailed
			assignment.Status.Error = "Agent unhealthy"
			assignment.UpdatedAt = time.Now()
			delete(c.assignments, id)
		}
	}
}

func (c *Controller) rebalanceDatasets() {
	c.agentsMu.RLock()
	agents := make([]*types.Agent, 0, len(c.agents))
	agentTimeout := types.ParseDuration(c.cfg.AgentTimeout, 5*time.Minute)
	for _, a := range c.agents {
		if a.IsHealthy(agentTimeout) {
			agents = append(agents, a)
		}
	}
	c.agentsMu.RUnlock()

	var lowSpaceAgents []*types.Agent
	var highSpaceAgents []*types.Agent

	for _, agent := range agents {
		utilizationPercent := float64(agent.State.DiskUsed) / float64(agent.State.DiskUsed+agent.State.DiskAvailable) * 100
		if utilizationPercent > 80 {
			lowSpaceAgents = append(lowSpaceAgents, agent)
		} else if utilizationPercent < 50 {
			highSpaceAgents = append(highSpaceAgents, agent)
		}
	}

	for _, lowAgent := range lowSpaceAgents {
		unassignedDatasets := lowAgent.GetDeletableDatasets("")
		for _, datasetKey := range unassignedDatasets {
			if len(highSpaceAgents) > 0 {
				targetAgent := highSpaceAgents[0]
				c.createRebalanceAssignment(datasetKey, lowAgent.ID, targetAgent.ID)
				break
			}
		}
	}
}

func (c *Controller) createRebalanceAssignment(datasetKey, fromAgent, toAgent string) {
	// Parse dataset key
	// Implementation for creating background copy assignments
}

func (c *Controller) CreateDataset(dataset *types.Dataset) error {
	c.datasetsMu.Lock()
	defer c.datasetsMu.Unlock()

	key := dataset.GetDatasetKey()
	if _, exists := c.datasets[key]; exists {
		return fmt.Errorf("dataset %s already exists", key)
	}

	dataset.Status.Phase = types.DatasetPhasePending
	dataset.CreatedAt = time.Now()
	dataset.UpdatedAt = time.Now()

	c.datasets[key] = dataset
	c.logger.Info("Created dataset", "dataset", key, "replication", dataset.Replication)

	return nil
}

func (c *Controller) GetDataset(name, revision string) (*types.Dataset, error) {
	c.datasetsMu.RLock()
	defer c.datasetsMu.RUnlock()

	key := fmt.Sprintf("%s@%s", name, revision)
	dataset, exists := c.datasets[key]
	if !exists {
		return nil, fmt.Errorf("dataset %s not found", key)
	}

	return dataset, nil
}

func (c *Controller) DeleteDataset(name, revision string) error {
	c.datasetsMu.Lock()
	defer c.datasetsMu.Unlock()

	key := fmt.Sprintf("%s@%s", name, revision)
	if _, exists := c.datasets[key]; !exists {
		return fmt.Errorf("dataset %s not found", key)
	}

	delete(c.datasets, key)

	c.assignmentsMu.Lock()
	for id, assignment := range c.assignments {
		if assignment.Dataset == name && assignment.Revision == revision {
			delete(c.assignments, id)
		}
	}
	c.assignmentsMu.Unlock()

	c.logger.Info("Deleted dataset", "dataset", key)
	return nil
}

func (c *Controller) RegisterAgent(agent *types.Agent) {
	c.agentsMu.Lock()
	defer c.agentsMu.Unlock()

	agent.LastHeartbeat = time.Now()
	c.agents[agent.ID] = agent
	c.logger.Info("Registered agent", "agent_id", agent.ID, "addr", agent.AdvertiseAddr)
}

func (c *Controller) UpdateAgentState(agentID string, state types.AgentState) error {
	c.agentsMu.Lock()
	defer c.agentsMu.Unlock()

	agent, exists := c.agents[agentID]
	if !exists {
		return fmt.Errorf("agent %s not found", agentID)
	}

	agent.State = state
	agent.LastHeartbeat = time.Now()

	c.updateAssignmentStatuses(agentID, state)

	return nil
}

func (c *Controller) updateAssignmentStatuses(agentID string, state types.AgentState) {
	c.assignmentsMu.Lock()
	defer c.assignmentsMu.Unlock()

	for _, assignmentID := range state.Assignments {
		if assignment, exists := c.assignments[assignmentID]; exists && assignment.AgentID == agentID {
			if localDataset, exists := state.LocalDatasets[assignment.Dataset+"@"+assignment.Revision]; exists {
				allReady := true
				for _, file := range localDataset.Files {
					if !file.IsComplete {
						allReady = false
						break
					}
				}

				if allReady {
					assignment.Status.Phase = types.AssignmentPhaseReady
				} else {
					assignment.Status.Phase = types.AssignmentPhaseDownloading
				}
				assignment.UpdatedAt = time.Now()
			}
		}
	}
}

func (c *Controller) GetAgentAssignments(agentID string) ([]*types.DatasetAssignment, error) {
	c.assignmentsMu.RLock()
	defer c.assignmentsMu.RUnlock()

	var assignments []*types.DatasetAssignment
	for _, assignment := range c.assignments {
		if assignment.AgentID == agentID {
			assignments = append(assignments, assignment)
		}
	}

	return assignments, nil
}

func (c *Controller) GetAgentsWithFile(dataset, revision, filePath string) ([]*types.Agent, error) {
	c.agentsMu.RLock()
	defer c.agentsMu.RUnlock()

	var agents []*types.Agent
	agentTimeout := types.ParseDuration(c.cfg.AgentTimeout, 5*time.Minute)
	for _, agent := range c.agents {
		if agent.IsHealthy(agentTimeout) && agent.HasFile(dataset, revision, filePath) {
			agents = append(agents, agent)
		}
	}

	return agents, nil
}

// GetAgentsWithBlock returns agents that have a specific block of a file
func (c *Controller) GetAgentsWithBlock(dataset, revision, filePath string, blockIndex int) ([]*types.Agent, error) {
	c.agentsMu.RLock()
	defer c.agentsMu.RUnlock()

	var agents []*types.Agent
	agentTimeout := types.ParseDuration(c.cfg.AgentTimeout, 5*time.Minute)
	datasetKey := fmt.Sprintf("%s@%s", dataset, revision)
	
	for _, agent := range c.agents {
		if !agent.IsHealthy(agentTimeout) {
			continue
		}
		
		// Check if agent has the specific block
		if datasetInfo, exists := agent.State.LocalDatasets[datasetKey]; exists {
			if fileInfo, exists := datasetInfo.Files[filePath]; exists {
				// Check if the block index is in the agent's block list
				for _, agentBlockIndex := range fileInfo.Blocks {
					if agentBlockIndex == blockIndex {
						agents = append(agents, agent)
						break
					}
				}
			}
		}
	}

	return agents, nil
}
