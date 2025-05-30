package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"syncbit/internal/config"
	"syncbit/internal/core/types"
	"syncbit/internal/transport"
)

// PeerProvider implements the Provider interface for peer-to-peer transfers
// Provides communication services for transferring blocks and files between agents
type PeerProvider struct {
	id           string
	cfg          *config.ProviderConfig
	token        string
	headers      map[string]string
	httpTransfer *transport.HTTPTransfer
}

// NewPeerProvider creates a new peer provider
func NewPeerProvider(cfg config.ProviderConfig, transferCfg config.TransferConfig) (Provider, error) {
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
func (p *PeerProvider) GetFileFromPeer(ctx context.Context, peerAddr types.Address, dataset, filepath string) ([]byte, error) {
	url := fmt.Sprintf("%s/datasets/%s/files/%s", peerAddr.URL(), dataset, filepath)

	var fileData []byte
	callback := func(resp *http.Response) error {
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("peer returned status %d", resp.StatusCode)
		}

		var err error
		fileData, err = io.ReadAll(resp.Body)
		return err
	}

	// Add peer auth headers if configured
	reqOpts := []transport.HTTPRequestOption{}
	if len(p.headers) > 0 {
		reqOpts = append(reqOpts, transport.HTTPRequestHeaders(p.headers))
	}

	err := p.httpTransfer.Get(ctx, url, callback, reqOpts...)
	return fileData, err
}

// CheckBlockFromPeer checks if a peer has a specific block (HEAD request)
func (p *PeerProvider) CheckBlockFromPeer(ctx context.Context, peerAddr types.Address, blockHash string) (bool, error) {
	url := fmt.Sprintf("%s/blocks/%s", peerAddr.URL(), blockHash)

	var exists bool
	callback := func(resp *http.Response) error {
		defer resp.Body.Close()
		exists = resp.StatusCode == http.StatusOK
		return nil
	}

	// Add peer auth headers if configured
	reqOpts := []transport.HTTPRequestOption{}
	if len(p.headers) > 0 {
		reqOpts = append(reqOpts, transport.HTTPRequestHeaders(p.headers))
	}

	err := p.httpTransfer.Head(ctx, url, callback, reqOpts...)
	return exists, err
}

// GetFileInfoFromPeer retrieves metadata for a file from a peer agent
func (p *PeerProvider) GetFileInfoFromPeer(ctx context.Context, peerAddr types.Address, dataset, filepath string) (*types.FileInfo, error) {
	url := fmt.Sprintf("%s/datasets/%s/files/%s", peerAddr.URL(), dataset, filepath)

	var fileInfo *types.FileInfo
	callback := func(resp *http.Response) error {
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("peer returned status %d", resp.StatusCode)
		}

		var response struct {
			File *types.FileInfo `json:"file"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
			return fmt.Errorf("failed to decode response: %w", err)
		}

		fileInfo = response.File
		return nil
	}

	// Add peer auth headers if configured
	reqOpts := []transport.HTTPRequestOption{}
	if len(p.headers) > 0 {
		reqOpts = append(reqOpts, transport.HTTPRequestHeaders(p.headers))
	}

	err := p.httpTransfer.Get(ctx, url, callback, reqOpts...)
	return fileInfo, err
}

// GetFileAvailabilityFromPeer checks if a peer has a complete file
func (p *PeerProvider) GetFileAvailabilityFromPeer(ctx context.Context, peerAddr types.Address, dataset, filepath string) (bool, error) {
	url := fmt.Sprintf("%s/datasets/%s/files/%s/info", peerAddr.URL(), dataset, filepath)

	var hasFile bool
	callback := func(resp *http.Response) error {
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusOK {
			hasFile = true
		} else if resp.StatusCode == http.StatusNotFound {
			hasFile = false
		} else {
			return fmt.Errorf("peer returned status %d", resp.StatusCode)
		}

		return nil
	}

	// Add peer auth headers if configured
	reqOpts := []transport.HTTPRequestOption{}
	if len(p.headers) > 0 {
		reqOpts = append(reqOpts, transport.HTTPRequestHeaders(p.headers))
	}

	err := p.httpTransfer.Get(ctx, url, callback, reqOpts...)
	return hasFile, err
}

// ListDatasetsFromPeer retrieves list of available datasets from a peer
func (p *PeerProvider) ListDatasetsFromPeer(ctx context.Context, peerAddr types.Address) ([]string, error) {
	url := fmt.Sprintf("%s/datasets", peerAddr.URL())

	var datasets []string
	callback := func(resp *http.Response) error {
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("peer returned status %d", resp.StatusCode)
		}

		var response struct {
			Datasets []struct {
				Name string `json:"name"`
			} `json:"datasets"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
			return fmt.Errorf("failed to decode response: %w", err)
		}

		datasets = make([]string, len(response.Datasets))
		for i, ds := range response.Datasets {
			datasets[i] = ds.Name
		}
		return nil
	}

	// Add peer auth headers if configured
	reqOpts := []transport.HTTPRequestOption{}
	if len(p.headers) > 0 {
		reqOpts = append(reqOpts, transport.HTTPRequestHeaders(p.headers))
	}

	err := p.httpTransfer.Get(ctx, url, callback, reqOpts...)
	return datasets, err
}

func init() {
	RegisterProviderFactory("peer", NewPeerProvider)
}
