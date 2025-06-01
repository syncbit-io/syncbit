package controller

import (
	"fmt"
	"sort"
	"time"

	"syncbit/internal/core/types"
)

type DatasetScheduler struct {
	controller *Controller
}

func NewDatasetScheduler(c *Controller) *DatasetScheduler {
	return &DatasetScheduler{
		controller: c,
	}
}

func (s *DatasetScheduler) ScheduleDataset(dataset *types.Dataset, count int) error {
	agents := s.getHealthyAgents()
	if len(agents) == 0 {
		return fmt.Errorf("no healthy agents available")
	}

	candidates := s.filterCandidateAgents(agents, dataset)
	if len(candidates) == 0 {
		return fmt.Errorf("no agents with sufficient capacity for dataset %s", dataset.GetDatasetKey())
	}

	s.sortAgentsByPriority(candidates, dataset)

	scheduled := 0
	for i := 0; i < len(candidates) && scheduled < count; i++ {
		agent := candidates[i]
		
		assignment := types.NewDatasetAssignment(
			agent.ID,
			dataset.Name,
			dataset.Revision,
			dataset.Priority,
		)

		s.controller.assignmentsMu.Lock()
		s.controller.assignments[assignment.ID] = assignment
		s.controller.assignmentsMu.Unlock()

		s.controller.logger.Info("Scheduled dataset assignment",
			"dataset", dataset.GetDatasetKey(),
			"agent", agent.ID,
			"assignment_id", assignment.ID,
		)

		scheduled++
	}

	if scheduled < count {
		s.controller.logger.Warn("Could not schedule all requested replicas",
			"dataset", dataset.GetDatasetKey(),
			"requested", count,
			"scheduled", scheduled,
		)
	}

	return nil
}

func (s *DatasetScheduler) getHealthyAgents() []*types.Agent {
	s.controller.agentsMu.RLock()
	defer s.controller.agentsMu.RUnlock()

	var agents []*types.Agent
	agentTimeout := types.ParseDuration(s.controller.cfg.AgentTimeout, 5*time.Minute)
	for _, agent := range s.controller.agents {
		if agent.IsHealthy(agentTimeout) {
			agents = append(agents, agent)
		}
	}
	return agents
}

func (s *DatasetScheduler) filterCandidateAgents(agents []*types.Agent, dataset *types.Dataset) []*types.Agent {
	var candidates []*types.Agent

	totalSize := s.calculateDatasetSize(dataset)

	for _, agent := range agents {
		if agent.HasDataset(dataset.Name, dataset.Revision) {
			continue
		}

		assignmentExists := false
		s.controller.assignmentsMu.RLock()
		for _, assignment := range s.controller.assignments {
			if assignment.AgentID == agent.ID && 
			   assignment.Dataset == dataset.Name && 
			   assignment.Revision == dataset.Revision {
				assignmentExists = true
				break
			}
		}
		s.controller.assignmentsMu.RUnlock()

		if assignmentExists {
			continue
		}

		if agent.HasCapacity(totalSize) {
			candidates = append(candidates, agent)
		}
	}

	return candidates
}

func (s *DatasetScheduler) calculateDatasetSize(dataset *types.Dataset) types.Bytes {
	var total types.Bytes
	for _, file := range dataset.Files {
		total += file.Size
	}
	return total
}

func (s *DatasetScheduler) sortAgentsByPriority(agents []*types.Agent, dataset *types.Dataset) {
	sort.Slice(agents, func(i, j int) bool {
		agentI := agents[i]
		agentJ := agents[j]

		fileCountI := s.countExistingFiles(agentI, dataset)
		fileCountJ := s.countExistingFiles(agentJ, dataset)
		if fileCountI != fileCountJ {
			return fileCountI > fileCountJ
		}

		loadI := agentI.GetAssignmentCount()
		loadJ := agentJ.GetAssignmentCount()
		if loadI != loadJ {
			return loadI < loadJ
		}

		availI := agentI.State.DiskAvailable
		availJ := agentJ.State.DiskAvailable
		return availI > availJ
	})
}

func (s *DatasetScheduler) countExistingFiles(agent *types.Agent, dataset *types.Dataset) int {
	count := 0
	for _, file := range dataset.Files {
		if agent.HasFile(dataset.Name, dataset.Revision, file.Path) {
			count++
		}
	}
	return count
}