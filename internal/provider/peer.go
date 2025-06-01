package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"syncbit/internal/api/request"
	"syncbit/internal/core/types"
	"syncbit/internal/transport"
)

// PeerProvider implements the Provider interface for peer-to-peer transfers
// It's essentially a simplified HTTP provider for communicating with other agents
type PeerProvider struct {
	id           string
	cfg          *types.ProviderConfig
	token        string
	headers      map[string]string
	httpTransfer *transport.HTTPTransfer
}

// NewPeerProvider creates a new peer provider
func NewPeerProvider(cfg types.ProviderConfig, transferCfg types.TransferConfig) (Provider, error) {
	// Create HTTP transfer for peer communication
	httpOpts := []transport.HTTPTransferOption{}
	if cfg.Token != "" {
		httpOpts = append(httpOpts, transport.HTTPWithClient(transport.DefaultHTTPClient()))
	}

	// Copy headers to avoid modifying the original config
	headers := make(map[string]string)
	for k, v := range cfg.Headers {
		headers[k] = v
	}

	// Add auth header if token is provided (for future peer authentication)
	if cfg.Token != "" {
		headers["Authorization"] = fmt.Sprintf("Bearer %s", cfg.Token)
		headers["X-Peer-Auth"] = cfg.Token
	}

	return &PeerProvider{
		id:           cfg.ID,
		cfg:          &cfg,
		token:        cfg.Token,
		headers:      headers,
		httpTransfer: transport.NewHTTPTransfer(httpOpts...),
	}, nil
}

// GetName returns the name of the provider
func (p *PeerProvider) GetName() string {
	if p.cfg.Name != "" {
		return p.cfg.Name
	}
	return p.cfg.Type
}

// GetID returns the unique ID of the provider
func (p *PeerProvider) GetID() string {
	return p.id
}

// GetToken returns the authentication token for this provider
func (p *PeerProvider) GetToken() string {
	return p.token
}

// GetHeaders returns the default headers for this provider
func (p *PeerProvider) GetHeaders() map[string]string {
	return p.headers
}

// GetFileFromPeer retrieves a complete file from a peer agent
func (p *PeerProvider) GetFileFromPeer(ctx context.Context, peerAddr *url.URL, dataset, filepath string) ([]byte, error) {
	fileURL := fmt.Sprintf("%s/datasets/%s/files/%s", peerAddr.String(), dataset, filepath)
	return p.httpGet(ctx, fileURL)
}

// GetFileInfoFromPeer retrieves metadata for a file from a peer agent
func (p *PeerProvider) GetFileInfoFromPeer(ctx context.Context, peerAddr *url.URL, dataset, filepath string) (*types.FileInfo, error) {
	apiURL := fmt.Sprintf("%s/datasets/%s/files/%s/info", peerAddr.String(), dataset, filepath)

	data, err := p.httpGet(ctx, apiURL)
	if err != nil {
		return nil, err
	}

	var response struct {
		File *types.FileInfo `json:"file"`
	}
	if err := json.Unmarshal(data, &response); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return response.File, nil
}

// GetFileAvailabilityFromPeer checks if a peer has a complete file
func (p *PeerProvider) GetFileAvailabilityFromPeer(ctx context.Context, peerAddr *url.URL, dataset, filepath string) (bool, error) {
	url := fmt.Sprintf("%s/datasets/%s/files/%s/info", peerAddr.String(), dataset, filepath)

	var hasFile bool
	err := p.httpTransfer.Get(ctx, url, func(resp *http.Response) error {
		defer resp.Body.Close()
		hasFile = resp.StatusCode == http.StatusOK
		return nil
	}, p.getRequestOptions()...)

	return hasFile, err
}

// ListDatasetsFromPeer retrieves list of available datasets from a peer
func (p *PeerProvider) ListDatasetsFromPeer(ctx context.Context, peerAddr *url.URL) ([]string, error) {
	apiURL := fmt.Sprintf("%s/datasets", peerAddr.String())

	data, err := p.httpGet(ctx, apiURL)
	if err != nil {
		return nil, err
	}

	var response struct {
		Datasets []struct {
			Name string `json:"name"`
		} `json:"datasets"`
	}
	if err := json.Unmarshal(data, &response); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	datasets := make([]string, len(response.Datasets))
	for i, ds := range response.Datasets {
		datasets[i] = ds.Name
	}
	return datasets, nil
}

// httpGet is a helper for simple GET requests that return data
func (p *PeerProvider) httpGet(ctx context.Context, url string) ([]byte, error) {
	var data []byte
	err := p.httpTransfer.Get(ctx, url, func(resp *http.Response) error {
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("peer returned status %d", resp.StatusCode)
		}

		var err error
		data, err = io.ReadAll(resp.Body)
		return err
	}, p.getRequestOptions()...)

	return data, err
}

// getRequestOptions returns the HTTP request options for peer communication
func (p *PeerProvider) getRequestOptions() []transport.HTTPRequestOption {
	if len(p.headers) > 0 {
		return []transport.HTTPRequestOption{request.WithHeaders(p.headers)}
	}
	return nil
}

// Download downloads a file from a peer using the provided ReaderWriter
func (p *PeerProvider) Download(ctx context.Context, repo, revision, filePath string, cacheWriter *types.ReaderWriter) error {
	// For peer provider, repo should be the peer address (e.g., "http://peer:8081")
	// and filePath is the file path within the dataset
	fileURL := fmt.Sprintf("%s/datasets/%s/files/%s", repo, url.QueryEscape(filePath), url.QueryEscape(filePath))

	// Create HTTP client
	client := &http.Client{}

	// Create request
	req, err := http.NewRequestWithContext(ctx, "GET", fileURL, nil)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	// Add default headers
	for k, v := range p.headers {
		req.Header.Set(k, v)
	}

	// Execute request
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("peer returned status %d", resp.StatusCode)
	}

	// Create a new ReaderWriter that combines the peer response with the cache writer
	opts := []types.RWOption{
		types.RWWithIOReader(resp.Body),
		types.RWWithIOWriter(cacheWriter.WriterAt(ctx)),
	}

	downloadRW := types.NewReaderWriter(opts...)

	// Transfer data from peer response through to cache
	_, err = downloadRW.Transfer(ctx)
	return err
}

func init() {
	RegisterProviderFactory("peer", NewPeerProvider)
}
