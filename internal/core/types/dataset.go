package types

import "time"

// DataSetInfo represents information about a dataset on an agent
type DataSetInfo struct {
	Name  string            `json:"name"`  // Dataset name (subdirectory)
	Files []DatasetFileInfo `json:"files"` // Files in this dataset
	Size  Bytes             `json:"size"`  // Total size of dataset in bytes
}

// DatasetFileInfo represents information about a file within a dataset
type DatasetFileInfo struct {
	Path         string    `json:"path"`          // Relative path within dataset
	Size         Bytes     `json:"size"`          // File size in bytes
	Status       string    `json:"status"`        // "complete", "downloading", "partial"
	LastModified time.Time `json:"last_modified"` // Last modification time
}
