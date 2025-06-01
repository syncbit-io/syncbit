package types

import (
	"fmt"
	"net/url"
	"sort"
	"time"
)

type AgentState struct {
	DiskUsed        Bytes                    `json:"disk_used"`
	DiskAvailable   Bytes                    `json:"disk_available"`
	Assignments     []string                 `json:"assignments"`
	LocalDatasets   map[string]LocalDataset  `json:"local_datasets"`
	LastUpdated     time.Time                `json:"last_updated"`
}

type LocalDataset struct {
	Name           string                     `json:"name"`
	Revision       string                     `json:"revision"`
	Files          map[string]LocalFileInfo   `json:"files"`
	TotalSize      Bytes                      `json:"total_size"`
	IsAssigned     bool                       `json:"is_assigned"`
	LastAccessed   time.Time                  `json:"last_accessed"`
}

type LocalFileInfo struct {
	Path        string    `json:"path"`
	Size        Bytes     `json:"size"`
	IsComplete  bool      `json:"is_complete"`
	Checksum    string    `json:"checksum,omitempty"`
	LastUpdated time.Time `json:"last_updated"`
	Blocks      []int     `json:"blocks,omitempty"` // List of block indices this agent has
}

type Agent struct {
	ID            string     `json:"id"`
	AdvertiseAddr *url.URL   `json:"advertise_addr"`
	LastHeartbeat time.Time  `json:"last_heartbeat"`
	State         AgentState `json:"state"`
}

func (a *Agent) IsHealthy(maxAge time.Duration) bool {
	return time.Since(a.LastHeartbeat) <= maxAge
}

func (a *Agent) HasCapacity(requiredSpace Bytes) bool {
	return a.State.DiskAvailable >= requiredSpace
}

func (a *Agent) GetAssignmentCount() int {
	return len(a.State.Assignments)
}

func (a *Agent) HasDataset(name, revision string) bool {
	key := fmt.Sprintf("%s@%s", name, revision)
	dataset, exists := a.State.LocalDatasets[key]
	if !exists {
		return false
	}
	
	for _, file := range dataset.Files {
		if !file.IsComplete {
			return false
		}
	}
	return true
}

func (a *Agent) HasFile(dataset, revision, filePath string) bool {
	key := fmt.Sprintf("%s@%s", dataset, revision)
	datasetInfo, exists := a.State.LocalDatasets[key]
	if !exists {
		return false
	}
	
	file, exists := datasetInfo.Files[filePath]
	return exists && file.IsComplete
}

func (a *Agent) GetDeletableDatasets(excludeDataset string) []string {
	var deletable []string
	for key, dataset := range a.State.LocalDatasets {
		if !dataset.IsAssigned && key != excludeDataset {
			deletable = append(deletable, key)
		}
	}
	
	sort.Slice(deletable, func(i, j int) bool {
		return a.State.LocalDatasets[deletable[i]].LastAccessed.Before(
			a.State.LocalDatasets[deletable[j]].LastAccessed)
	})
	
	return deletable
}