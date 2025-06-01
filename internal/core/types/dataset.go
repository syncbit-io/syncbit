package types

import (
	"fmt"
	"time"
)

type Dataset struct {
	Name        string            `json:"name"`
	Source      string            `json:"source,omitempty"` // Provider-specific source identifier (repo, URL, bucket, etc.)
	Revision    string            `json:"revision"`
	Files       []DatasetFile     `json:"files"`
	Replication int               `json:"replication"`
	Sources     []ProviderConfig  `json:"sources"`
	Priority    int               `json:"priority"`
	Status      DatasetStatus     `json:"status"`
	CreatedAt   time.Time         `json:"created_at"`
	UpdatedAt   time.Time         `json:"updated_at"`
}

type DatasetFile struct {
	Path     string      `json:"path"`
	Size     Bytes       `json:"size"`
	Checksum string      `json:"checksum,omitempty"`
	ModTime  *time.Time  `json:"mod_time,omitempty"`
	ETag     string      `json:"etag,omitempty"`
}

type DatasetStatus struct {
	Phase          DatasetPhase                   `json:"phase"`
	ReadyReplicas  int                           `json:"ready_replicas"`
	TotalReplicas  int                           `json:"total_replicas"`
	Assignments    map[string]*AssignmentStatus  `json:"assignments"`
	LastUpdated    time.Time                     `json:"last_updated"`
	Message        string                        `json:"message"`
}

type DatasetPhase string

const (
	DatasetPhasePending      DatasetPhase = "Pending"
	DatasetPhaseScheduling   DatasetPhase = "Scheduling"
	DatasetPhaseDownloading  DatasetPhase = "Downloading"
	DatasetPhaseReady        DatasetPhase = "Ready"
	DatasetPhaseFailed       DatasetPhase = "Failed"
)

type DatasetAssignment struct {
	ID        string           `json:"id"`
	AgentID   string           `json:"agent_id"`
	Dataset   string           `json:"dataset"`
	Revision  string           `json:"revision"`
	Priority  int              `json:"priority"`
	Status    AssignmentStatus `json:"status"`
	CreatedAt time.Time        `json:"created_at"`
	UpdatedAt time.Time        `json:"updated_at"`
}

type AssignmentStatus struct {
	Phase        AssignmentPhase           `json:"phase"`
	Files        map[string]*FileStatus    `json:"files"`
	Progress     AssignmentProgress        `json:"progress"`
	LastUpdated  time.Time                 `json:"last_updated"`
	Message      string                    `json:"message"`
	Error        string                    `json:"error"`
}

type AssignmentPhase string

const (
	AssignmentPhasePending      AssignmentPhase = "Pending"
	AssignmentPhaseReconciling  AssignmentPhase = "Reconciling"
	AssignmentPhaseDownloading  AssignmentPhase = "Downloading"
	AssignmentPhaseReady        AssignmentPhase = "Ready"
	AssignmentPhaseFailed       AssignmentPhase = "Failed"
)

type FileStatus struct {
	Phase        FilePhase   `json:"phase"`
	BytesTotal   Bytes       `json:"bytes_total"`
	BytesStored  Bytes       `json:"bytes_stored"`
	Sources      []string    `json:"sources"`
	LastUpdated  time.Time   `json:"last_updated"`
	Error        string      `json:"error"`
}

type FilePhase string

const (
	FilePhasePending      FilePhase = "Pending"
	FilePhaseDiscovering  FilePhase = "Discovering"
	FilePhaseDownloading  FilePhase = "Downloading"
	FilePhaseVerifying    FilePhase = "Verifying"
	FilePhaseReady        FilePhase = "Ready"
	FilePhaseFailed       FilePhase = "Failed"
)

type AssignmentProgress struct {
	FilesTotal     int     `json:"files_total"`
	FilesReady     int     `json:"files_ready"`
	FilesFailed    int     `json:"files_failed"`
	BytesTotal     Bytes   `json:"bytes_total"`
	BytesStored    Bytes   `json:"bytes_stored"`
	PercentReady   float64 `json:"percent_ready"`
	PercentBytes   float64 `json:"percent_bytes"`
}

func NewDataset(name, revision string, replication int) *Dataset {
	now := time.Now()
	return &Dataset{
		Name:        name,
		Revision:    revision,
		Files:       make([]DatasetFile, 0),
		Replication: replication,
		Sources:     make([]ProviderConfig, 0),
		Priority:    0,
		Status: DatasetStatus{
			Phase:       DatasetPhasePending,
			Assignments: make(map[string]*AssignmentStatus),
		},
		CreatedAt: now,
		UpdatedAt: now,
	}
}

func NewDatasetAssignment(agentID, dataset, revision string, priority int) *DatasetAssignment {
	now := time.Now()
	return &DatasetAssignment{
		ID:       GenerateAssignmentID(agentID, dataset, revision),
		AgentID:  agentID,
		Dataset:  dataset,
		Revision: revision,
		Priority: priority,
		Status: AssignmentStatus{
			Phase:       AssignmentPhasePending,
			Files:       make(map[string]*FileStatus),
			LastUpdated: now,
		},
		CreatedAt: now,
		UpdatedAt: now,
	}
}

func GenerateAssignmentID(agentID, dataset, revision string) string {
	return fmt.Sprintf("%s-%s-%s", agentID, dataset, revision)
}

func (d *Dataset) GetDatasetKey() string {
	return fmt.Sprintf("%s@%s", d.Name, d.Revision)
}

func (d *Dataset) IsReady() bool {
	return d.Status.Phase == DatasetPhaseReady && d.Status.ReadyReplicas >= d.Replication
}

func (d *Dataset) NeedsMoreReplicas() bool {
	return d.Status.ReadyReplicas < d.Replication
}

func (a *DatasetAssignment) IsReady() bool {
	return a.Status.Phase == AssignmentPhaseReady
}

func (a *DatasetAssignment) IsFailed() bool {
	return a.Status.Phase == AssignmentPhaseFailed
}

func (s *AssignmentStatus) UpdateProgress() {
	s.Progress.FilesTotal = len(s.Files)
	s.Progress.FilesReady = 0
	s.Progress.FilesFailed = 0
	s.Progress.BytesTotal = 0
	s.Progress.BytesStored = 0

	for _, fileStatus := range s.Files {
		s.Progress.BytesTotal += fileStatus.BytesTotal
		s.Progress.BytesStored += fileStatus.BytesStored

		switch fileStatus.Phase {
		case FilePhaseReady:
			s.Progress.FilesReady++
		case FilePhaseFailed:
			s.Progress.FilesFailed++
		}
	}

	if s.Progress.FilesTotal > 0 {
		s.Progress.PercentReady = float64(s.Progress.FilesReady) / float64(s.Progress.FilesTotal) * 100
	}
	if s.Progress.BytesTotal > 0 {
		s.Progress.PercentBytes = float64(s.Progress.BytesStored) / float64(s.Progress.BytesTotal) * 100
	}

	s.LastUpdated = time.Now()
}