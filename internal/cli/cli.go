package cli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// JobConfig represents the configuration for a download job
type JobConfig struct {
	ProviderID string   `json:"provider_id"`
	Repo       string   `json:"repo"`
	Revision   string   `json:"revision"`
	Files      []string `json:"files"`
	LocalPath  string   `json:"local_path"`
}

// Job represents a download job
type Job struct {
	ID      string    `json:"id"`
	Type    string    `json:"type"`
	Handler string    `json:"handler"`
	Config  JobConfig `json:"config"`
	Status  string    `json:"status"`
	Error   string    `json:"error,omitempty"`
}

type Client struct {
	ControllerURL string
	httpClient    *http.Client
}

func NewClient(url string) *Client {
	if url == "" {
		url = "http://localhost:8080"
	}
	return &Client{
		ControllerURL: url,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// SubmitJob submits a new download job to the controller
func (c *Client) SubmitJob(repo string, files []string, localPath string, providerID string) error {
	job := Job{
		Type:    "download",
		Handler: "hf",
		Config: JobConfig{
			ProviderID: providerID,
			Repo:       repo,
			Revision:   "main",
			Files:      files,
			LocalPath:  localPath,
		},
	}

	jsonData, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("failed to marshal job: %w", err)
	}

	url := fmt.Sprintf("%s/jobs", c.ControllerURL)
	resp, err := c.httpClient.Post(url, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("failed to submit job: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("failed to submit job, status: %d", resp.StatusCode)
	}

	var response struct {
		Message string `json:"message"`
		JobID   string `json:"job_id"`
		Status  string `json:"status"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return fmt.Errorf("failed to decode response: %w", err)
	}

	fmt.Printf("Job submitted successfully!\n")
	fmt.Printf("Job ID: %s\n", response.JobID)
	fmt.Printf("Status: %s\n", response.Status)

	return nil
}

// ListJobs lists all jobs from the controller
func (c *Client) ListJobs() error {
	url := fmt.Sprintf("%s/jobs", c.ControllerURL)
	resp, err := c.httpClient.Get(url)
	if err != nil {
		return fmt.Errorf("failed to list jobs: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to list jobs, status: %d", resp.StatusCode)
	}

	var response struct {
		Jobs  []Job `json:"jobs"`
		Count int   `json:"count"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return fmt.Errorf("failed to decode response: %w", err)
	}

	fmt.Printf("Found %d jobs:\n\n", response.Count)
	for _, job := range response.Jobs {
		fmt.Printf("Job ID: %s\n", job.ID)
		fmt.Printf("Type: %s\n", job.Type)
		fmt.Printf("Handler: %s\n", job.Handler)
		fmt.Printf("Status: %s\n", job.Status)
		fmt.Printf("Repository: %s\n", job.Config.Repo)
		fmt.Printf("Files: %v\n", job.Config.Files)
		fmt.Printf("Local Path: %s\n", job.Config.LocalPath)
		if job.Error != "" {
			fmt.Printf("Error: %s\n", job.Error)
		}
		fmt.Println("---")
	}

	return nil
}

// GetJob gets a specific job by ID
func (c *Client) GetJob(jobID string) error {
	url := fmt.Sprintf("%s/jobs/%s", c.ControllerURL, jobID)
	resp, err := c.httpClient.Get(url)
	if err != nil {
		return fmt.Errorf("failed to get job: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return fmt.Errorf("job not found: %s", jobID)
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to get job, status: %d", resp.StatusCode)
	}

	var response struct {
		Job Job `json:"job"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return fmt.Errorf("failed to decode response: %w", err)
	}

	job := response.Job
	fmt.Printf("Job Details:\n")
	fmt.Printf("ID: %s\n", job.ID)
	fmt.Printf("Type: %s\n", job.Type)
	fmt.Printf("Handler: %s\n", job.Handler)
	fmt.Printf("Status: %s\n", job.Status)
	fmt.Printf("Repository: %s\n", job.Config.Repo)
	fmt.Printf("Revision: %s\n", job.Config.Revision)
	fmt.Printf("Provider ID: %s\n", job.Config.ProviderID)
	fmt.Printf("Files: %v\n", job.Config.Files)
	fmt.Printf("Local Path: %s\n", job.Config.LocalPath)
	if job.Error != "" {
		fmt.Printf("Error: %s\n", job.Error)
	}

	return nil
}

// List is for backward compatibility - alias to ListJobs
func (c *Client) List() error {
	return c.ListJobs()
}
