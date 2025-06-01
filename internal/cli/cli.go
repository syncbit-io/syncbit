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

// CreateDataset creates a new dataset on the controller
func (c *Client) CreateDataset(ctx context.Context, name, revision string, replication int, sources []types.ProviderConfig, files []types.DatasetFile) (*types.Dataset, error) {
	dataset := &types.Dataset{
		Name:        name,
		Revision:    revision,
		Replication: replication,
		Sources:     sources,
		Files:       files,
	}

	url := c.ControllerURL + "/datasets"
	var response map[string]any

	err := c.httpClient.Post(ctx, url, func(resp *http.Response) error {
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusCreated {
			return fmt.Errorf("dataset creation failed with status: %d", resp.StatusCode)
		}

		return json.NewDecoder(resp.Body).Decode(&response)
	}, request.WithJSON(dataset))

	if err != nil {
		return nil, fmt.Errorf("failed to create dataset: %w", err)
	}

	// Extract dataset from response
	if datasetData, ok := response["dataset"]; ok {
		datasetBytes, _ := json.Marshal(datasetData)
		var returnedDataset types.Dataset
		if err := json.Unmarshal(datasetBytes, &returnedDataset); err == nil {
			return &returnedDataset, nil
		}
	}

	return dataset, nil
}

// GetDataset retrieves a dataset by name and revision
func (c *Client) GetDataset(ctx context.Context, name, revision string) (*types.Dataset, error) {
	url := fmt.Sprintf("%s/datasets/%s/%s", c.ControllerURL, name, revision)
	var response struct {
		Dataset *types.Dataset `json:"dataset"`
	}

	err := c.httpClient.Get(ctx, url, func(resp *http.Response) error {
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("dataset not found or error: %d", resp.StatusCode)
		}

		return json.NewDecoder(resp.Body).Decode(&response)
	})

	if err != nil {
		return nil, fmt.Errorf("failed to get dataset: %w", err)
	}

	return response.Dataset, nil
}

// ListDatasets retrieves all datasets
func (c *Client) ListDatasets(ctx context.Context) ([]*types.Dataset, error) {
	url := c.ControllerURL + "/datasets"
	var response struct {
		Datasets []*types.Dataset `json:"datasets"`
	}

	err := c.httpClient.Get(ctx, url, func(resp *http.Response) error {
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("failed to list datasets: %d", resp.StatusCode)
		}

		return json.NewDecoder(resp.Body).Decode(&response)
	})

	if err != nil {
		return nil, fmt.Errorf("failed to list datasets: %w", err)
	}

	return response.Datasets, nil
}

// DeleteDataset deletes a dataset by name and revision
func (c *Client) DeleteDataset(ctx context.Context, name, revision string) error {
	url := fmt.Sprintf("%s/datasets/%s/%s", c.ControllerURL, name, revision)

	err := c.httpClient.Delete(ctx, url, func(resp *http.Response) error {
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("dataset deletion failed with status: %d", resp.StatusCode)
		}

		return nil
	})

	if err != nil {
		return fmt.Errorf("failed to delete dataset: %w", err)
	}

	return nil
}

// ListAgents retrieves all registered agents
func (c *Client) ListAgents(ctx context.Context) ([]*types.Agent, error) {
	url := c.ControllerURL + "/agents"
	var response struct {
		Agents []*types.Agent `json:"agents"`
	}

	err := c.httpClient.Get(ctx, url, func(resp *http.Response) error {
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("failed to list agents: %d", resp.StatusCode)
		}

		return json.NewDecoder(resp.Body).Decode(&response)
	})

	if err != nil {
		return nil, fmt.Errorf("failed to list agents: %w", err)
	}

	return response.Agents, nil
}

// GetAgentAssignments retrieves assignments for a specific agent
func (c *Client) GetAgentAssignments(ctx context.Context, agentID string) ([]*types.DatasetAssignment, error) {
	url := fmt.Sprintf("%s/agents/%s/assignments", c.ControllerURL, agentID)
	var response struct {
		Assignments []*types.DatasetAssignment `json:"assignments"`
	}

	err := c.httpClient.Get(ctx, url, func(resp *http.Response) error {
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("failed to get assignments: %d", resp.StatusCode)
		}

		return json.NewDecoder(resp.Body).Decode(&response)
	})

	if err != nil {
		return nil, fmt.Errorf("failed to get agent assignments: %w", err)
	}

	return response.Assignments, nil
}

// GetFilePeers retrieves agents that have a specific file
func (c *Client) GetFilePeers(ctx context.Context, dataset, revision, filePath string) ([]map[string]any, error) {
	url := fmt.Sprintf("%s/files/%s/%s/%s/peers", c.ControllerURL, dataset, revision, filePath)
	var response struct {
		Peers []map[string]any `json:"peers"`
	}

	err := c.httpClient.Get(ctx, url, func(resp *http.Response) error {
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("failed to get file peers: %d", resp.StatusCode)
		}

		return json.NewDecoder(resp.Body).Decode(&response)
	})

	if err != nil {
		return nil, fmt.Errorf("failed to get file peers: %w", err)
	}

	return response.Peers, nil
}

// GetControllerHealth checks controller health
func (c *Client) GetControllerHealth(ctx context.Context) (map[string]any, error) {
	url := c.ControllerURL + "/health"
	var response map[string]any

	err := c.httpClient.Get(ctx, url, func(resp *http.Response) error {
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("health check failed with status: %d", resp.StatusCode)
		}

		return json.NewDecoder(resp.Body).Decode(&response)
	})

	if err != nil {
		return nil, fmt.Errorf("failed to get controller health: %w", err)
	}

	return response, nil
}
