package provider

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"syncbit/internal/api/request"
	"syncbit/internal/config"
	"syncbit/internal/core/types"
	"syncbit/internal/transport"
)

// HTTPProvider implements the Provider interface for HTTP/HTTPS endpoints
// Provides authentication and connection services for HTTP access
type HTTPProvider struct {
	id           string
	cfg          *config.ProviderConfig
	token        string
	headers      map[string]string
	httpTransfer *transport.HTTPTransfer
}

func NewHTTPProvider(cfg config.ProviderConfig, transferCfg config.TransferConfig) (Provider, error) {
	// Create HTTP transfer with optional auth token
	httpOpts := []transport.HTTPTransferOption{}
	if cfg.Token != "" {
		httpOpts = append(httpOpts, transport.HTTPWithClient(transport.DefaultHTTPClient()))
	}

	// Copy headers to avoid modifying the original config
	headers := make(map[string]string)
	for k, v := range cfg.Headers {
		headers[k] = v
	}

	// Add auth header if token is provided
	if cfg.Token != "" {
		headers["Authorization"] = fmt.Sprintf("Bearer %s", cfg.Token)
	}

	return &HTTPProvider{
		id:           cfg.ID,
		cfg:          &cfg,
		token:        cfg.Token,
		headers:      headers,
		httpTransfer: transport.NewHTTPTransfer(httpOpts...),
	}, nil
}

func (p *HTTPProvider) GetName() string {
	if p.cfg.Name != "" {
		return p.cfg.Name
	}
	return p.cfg.Type
}

func (p *HTTPProvider) GetID() string {
	return p.id
}

// GetToken returns the authentication token for this provider
func (p *HTTPProvider) GetToken() string {
	return p.token
}

// GetHeaders returns the default headers for this provider
func (p *HTTPProvider) GetHeaders() map[string]string {
	return p.headers
}

// GetFileFromURL retrieves metadata for a file at the specified URL
func (p *HTTPProvider) GetFileFromURL(ctx context.Context, fileURL string) (*types.FileInfo, error) {
	var fileInfo *types.FileInfo

	callback := func(resp *http.Response) error {
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("unexpected status: %s", resp.Status)
		}

		// Parse URL to get file path
		parsedURL, err := url.Parse(fileURL)
		if err != nil {
			return fmt.Errorf("failed to parse URL: %w", err)
		}

		// Get the Last-Modified header and parse it if available
		var modTime time.Time
		lastModStr := resp.Header.Get("Last-Modified")
		if lastModStr != "" {
			modTime, err = http.ParseTime(lastModStr)
			if err != nil {
				modTime = time.Now()
			}
		} else {
			modTime = time.Now()
		}

		fileInfo = &types.FileInfo{
			Path:    parsedURL.Path,
			Size:    types.Bytes(resp.ContentLength),
			ETag:    resp.Header.Get("ETag"),
			ModTime: modTime,
		}
		return nil
	}

	// Add default headers
	reqOpts := []transport.HTTPRequestOption{}
	if len(p.headers) > 0 {
		reqOpts = append(reqOpts, request.WithHeaders(p.headers))
	}

	err := p.httpTransfer.Head(ctx, fileURL, callback, reqOpts...)
	return fileInfo, err
}

func init() {
	RegisterProviderFactory("http", NewHTTPProvider)
}
