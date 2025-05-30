package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"syncbit/internal/api/request"
	"syncbit/internal/core/types"
	"syncbit/internal/transport"
)

type Client struct {
	ControllerURL string
	httpClient    *transport.HTTPTransfer
}

func NewClient(url string) *Client {
	if url == "" {
		url = "http://localhost:8080"
	}
	return &Client{
		ControllerURL: url,
		httpClient:    transport.NewHTTPTransfer(),
	}
}

// SubmitJob submits a new download job to the controller
func (c *Client) SubmitJob(jobID string, config types.JobConfig, handler types.JobHandler) (*types.Job, error) {
	job := types.NewJob(jobID, handler, config)

	ctx := context.Background()
	url := c.ControllerURL + "/jobs"
	var response map[string]interface{}

	err := c.httpClient.Post(ctx, url, func(resp *http.Response) error {
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusCreated {
			return fmt.Errorf("job submission failed with status: %d", resp.StatusCode)
		}

		return json.NewDecoder(resp.Body).Decode(&response)
	}, request.WithJSON(job))

	if err != nil {
		return nil, fmt.Errorf("failed to submit job: %w", err)
	}

	// Return the job we submitted since the response format may vary
	return job, nil
}

// GetJob retrieves a job by ID
func (c *Client) GetJob(jobID string) (*types.Job, error) {
	ctx := context.Background()
	url := fmt.Sprintf("%s/jobs/%s", c.ControllerURL, jobID)
	var job types.Job

	err := c.httpClient.Get(ctx, url, func(resp *http.Response) error {
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("job not found or error: %d", resp.StatusCode)
		}

		return json.NewDecoder(resp.Body).Decode(&job)
	})

	if err != nil {
		return nil, fmt.Errorf("failed to get job: %w", err)
	}

	return &job, nil
}

// ListJobs retrieves all jobs
func (c *Client) ListJobs() ([]*types.Job, error) {
	ctx := context.Background()
	url := c.ControllerURL + "/jobs"
	var response struct {
		Jobs []*types.Job `json:"jobs"`
	}

	err := c.httpClient.Get(ctx, url, func(resp *http.Response) error {
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("failed to list jobs: %d", resp.StatusCode)
		}

		return json.NewDecoder(resp.Body).Decode(&response)
	})

	if err != nil {
		return nil, fmt.Errorf("failed to list jobs: %w", err)
	}

	return response.Jobs, nil
}

// CancelJob cancels a job by ID
func (c *Client) CancelJob(jobID string) error {
	ctx := context.Background()
	url := fmt.Sprintf("%s/jobs/%s", c.ControllerURL, jobID)

	err := c.httpClient.Delete(ctx, url, func(resp *http.Response) error {
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("job cancellation failed with status: %d", resp.StatusCode)
		}

		return nil
	})

	if err != nil {
		return fmt.Errorf("failed to cancel job: %w", err)
	}

	return nil
}
