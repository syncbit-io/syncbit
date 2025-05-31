package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"time"

	"syncbit/internal/core/types"
	"syncbit/internal/transport"
)

const (
	hfBaseURL   = "https://huggingface.co"
	hfApiModels = "api/models"
	hfResolve   = "resolve"
)

// HFProvider implements the Provider interface for Hugging Face.
// Provides authentication and metadata services for HuggingFace repositories.
type HFProvider struct {
	id           string
	cfg          *types.ProviderConfig
	transferCfg  types.TransferConfig
	token        string
	httpTransfer *transport.HTTPTransfer
}

// NewHFProvider creates a new HFProvider.
// Configures authentication for HuggingFace API access.
func NewHFProvider(cfg types.ProviderConfig, transferCfg types.TransferConfig) (Provider, error) {
	// Create HTTP transfer with rate limiting
	httpOpts := []transport.HTTPTransferOption{}
	
	// Create rate limiter based on transfer config
	if transferCfg.RateLimit > 0 {
		limiter := types.NewRateLimiter(types.Bytes(transferCfg.RateLimit))
		httpOpts = append(httpOpts, transport.HTTPWithRateLimiter(limiter))
	}

	return &HFProvider{
		id:           cfg.ID,
		cfg:          &cfg,
		transferCfg:  transferCfg,
		token:        cfg.Token,
		httpTransfer: transport.NewHTTPTransfer(httpOpts...),
	}, nil
}

// GetName returns the name of the provider
func (p *HFProvider) GetName() string {
	if p.cfg.Name != "" {
		return p.cfg.Name
	}
	return p.cfg.Type
}

// GetID returns the unique ID of the provider
func (p *HFProvider) GetID() string {
	return p.id
}

// GetToken returns the authentication token for this provider
func (p *HFProvider) GetToken() string {
	return p.token
}

// ListFilesForRepo lists all filenames for the specified model repository.
// Uses the Hugging Face API to retrieve the file list from the repository.
func (p *HFProvider) ListFilesForRepo(ctx context.Context, repo string) ([]string, error) {
	url, err := url.JoinPath(hfBaseURL, hfApiModels, repo)
	if err != nil {
		return nil, fmt.Errorf("join path: %w", err)
	}

	var files []string

	callback := func(resp *http.Response) error {
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("unexpected status: %s", resp.Status)
		}

		var result struct {
			Siblings []struct {
				Rfilename string `json:"rfilename"`
			} `json:"siblings"`
		}

		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			return fmt.Errorf("decode response: %w", err)
		}

		files = make([]string, 0, len(result.Siblings))
		for _, sibling := range result.Siblings {
			files = append(files, sibling.Rfilename)
		}
		return nil
	}

	// Add auth header if token is provided
	reqOpts := []transport.HTTPRequestOption{}
	if p.token != "" {
		reqOpts = append(reqOpts, transport.HTTPRequestHeaders(map[string]string{
			"Authorization": fmt.Sprintf("Bearer %s", p.token),
		}))
	}

	err = p.httpTransfer.Get(ctx, url, callback, reqOpts...)
	return files, err
}

// GetFileInfo retrieves metadata for a specific file in a repository.
// Uses a HEAD request to get file size, ETag, and Last-Modified headers.
func (p *HFProvider) GetFileInfo(ctx context.Context, repo, revision, path string) (*types.FileInfo, error) {
	url, err := url.JoinPath(hfBaseURL, repo, hfResolve, revision, path)
	if err != nil {
		return nil, fmt.Errorf("join path: %w", err)
	}

	slog.Debug("Getting file", "url", url)

	var fileInfo *types.FileInfo

	callback := func(resp *http.Response) error {
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("unexpected status: %s", resp.Status)
		}

		// Get the Last-Modified header and parse it if available
		var modTime time.Time
		lastModStr := resp.Header.Get("Last-Modified")
		if lastModStr != "" {
			// Parse the HTTP date format
			modTime, err = http.ParseTime(lastModStr)
			if err != nil {
				// If we can't parse it, just use current time
				modTime = time.Now()
			}
		} else {
			modTime = time.Now()
		}

		fileInfo = &types.FileInfo{
			Path:    path,
			Size:    types.Bytes(resp.ContentLength),
			ETag:    resp.Header.Get("ETag"),
			ModTime: modTime,
		}
		return nil
	}

	// Add auth header if token is provided
	reqOpts := []transport.HTTPRequestOption{}
	if p.token != "" {
		reqOpts = append(reqOpts, transport.HTTPRequestHeaders(map[string]string{
			"Authorization": fmt.Sprintf("Bearer %s", p.token),
		}))
	}

	err = p.httpTransfer.Head(ctx, url, callback, reqOpts...)
	return fileInfo, err
}

// BuildDownloadURL constructs the download URL for a file
func (p *HFProvider) BuildDownloadURL(repo, revision, path string) string {
	return fmt.Sprintf("%s/%s/%s/%s/%s", hfBaseURL, repo, hfResolve, revision, path)
}

func init() {
	RegisterProviderFactory("hf", NewHFProvider)
}
