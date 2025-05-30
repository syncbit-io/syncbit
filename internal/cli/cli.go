package cli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"syncbit/internal/core/types"
)

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
func (c *Client) SubmitJob(config types.JobConfig, handler types.JobHandler) (*types.Job, error) {
	job := types.NewJob("", handler, config)

	jsonData, err := json.Marshal(job)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal job: %w", err)
	}

	resp, err := c.httpClient.Post(c.ControllerURL+"/jobs", "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, fmt.Errorf("failed to submit job: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		return nil, fmt.Errorf("job submission failed with status: %d", resp.StatusCode)
	}

	var submittedJob types.Job
	if err := json.NewDecoder(resp.Body).Decode(&submittedJob); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &submittedJob, nil
}

// GetJob retrieves a job by ID
func (c *Client) GetJob(jobID string) (*types.Job, error) {
	resp, err := c.httpClient.Get(fmt.Sprintf("%s/jobs/%s", c.ControllerURL, jobID))
	if err != nil {
		return nil, fmt.Errorf("failed to get job: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("job not found or error: %d", resp.StatusCode)
	}

	var job types.Job
	if err := json.NewDecoder(resp.Body).Decode(&job); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &job, nil
}

// ListJobs retrieves all jobs
func (c *Client) ListJobs() ([]*types.Job, error) {
	resp, err := c.httpClient.Get(c.ControllerURL + "/jobs")
	if err != nil {
		return nil, fmt.Errorf("failed to list jobs: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to list jobs: %d", resp.StatusCode)
	}

	var response struct {
		Jobs []*types.Job `json:"jobs"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return response.Jobs, nil
}

// CancelJob cancels a job by ID
func (c *Client) CancelJob(jobID string) error {
	req, err := http.NewRequest("DELETE", fmt.Sprintf("%s/jobs/%s", c.ControllerURL, jobID), nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to cancel job: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("job cancellation failed with status: %d", resp.StatusCode)
	}

	return nil
}
