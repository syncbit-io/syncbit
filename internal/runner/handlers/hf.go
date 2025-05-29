package handlers

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"

	"syncbit/internal/core/types"
	"syncbit/internal/provider"
	"syncbit/internal/runner"
	"syncbit/internal/transport"
)

type HFDownloadHandlerOption func(*HFDownloadHandler)

func HFDownloadHandlerWithLimiter(limiter *types.RateLimiter) HFDownloadHandlerOption {
	return func(h *HFDownloadHandler) {
		h.limiter = limiter
	}
}

// HFDownloadHandler downloads files from HuggingFace using a configured provider
type HFDownloadHandler struct {
	localBasePath string // local base path
	localJobPath  string // local job path -> localBasePath/localJobPath
	providerID    string // ID of the HF provider to use
	repo          string // HuggingFace repository (e.g., "meta-llama/Llama-2-7b-hf")
	revision      string // Repository revision (e.g., "main", "v1.0")
	remotePath    string // remote file path within the repository
	limiter       *types.RateLimiter
}

// NewHFDownloadHandler creates a new HFDownloadHandler for downloading files from HuggingFace
func NewHFDownloadHandler(
	localBasePath, localJobPath,
	providerID, repo, revision, remotePath string,
	opts ...HFDownloadHandlerOption,
) *HFDownloadHandler {
	h := &HFDownloadHandler{
		localBasePath: localBasePath,
		localJobPath:  localJobPath,
		providerID:    providerID,
		repo:          repo,
		revision:      revision,
		remotePath:    remotePath,
		limiter:       types.DefaultRateLimiter(),
	}

	for _, opt := range opts {
		opt(h)
	}

	return h
}

func (h *HFDownloadHandler) Run(ctx context.Context, self *runner.Job) error {
	// Get the provider from the registry
	prov, err := provider.GetProvider(h.providerID)
	if err != nil {
		return fmt.Errorf("failed to get provider %s: %w", h.providerID, err)
	}

	// Cast to HFProvider to access HF-specific methods
	hfProv, ok := prov.(*provider.HFProvider)
	if !ok {
		return fmt.Errorf("provider %s is not an HF provider", h.providerID)
	}

	// Get file metadata to set up progress tracking
	fileInfo, err := hfProv.GetFileInfo(ctx, h.repo, h.revision, h.remotePath)
	if err != nil {
		return fmt.Errorf("failed to get file info: %w", err)
	}

	// Create destination directory
	filePath := filepath.Dir(h.remotePath)
	fileName := filepath.Base(h.remotePath)
	destPath := filepath.Join(h.localBasePath, h.localJobPath, filePath)

	if err := os.MkdirAll(destPath, 0755); err != nil {
		return err
	}

	destFile := filepath.Join(destPath, fileName)
	fileHandle, err := os.Create(destFile)
	if err != nil {
		return err
	}
	defer fileHandle.Close()

	// Create HTTP transport for the actual download
	ht := transport.NewHTTPTransfer(
		transport.HTTPWithClient(transport.DefaultHTTPClient()),
	)

	// Build download URL using the provider's method
	downloadURL := hfProv.BuildDownloadURL(h.repo, h.revision, h.remotePath)

	htCallback := func(resp *http.Response) error {
		defer resp.Body.Close()

		// Set up progress tracking
		if fileInfo != nil {
			self.Tracker().SetTotal(int64(fileInfo.Size))
		} else {
			self.Tracker().SetTotal(resp.ContentLength)
		}
		self.Tracker().SetCurrent(0)

		// Create reader/writer with progress tracking
		rw := types.NewReaderWriter(
			types.RWWithIOWriter(fileHandle),
			types.RWWithIOReader(resp.Body),
			types.RWWithReadLimiter(h.limiter),
			types.RWWithReaderCallback(func(n int64) {
				self.Tracker().IncCurrent(n)
			}),
		)

		_, err := rw.Transfer(ctx)
		return err
	}

	// Add authorization header using the provider's token
	reqOpts := []transport.HTTPRequestOption{}
	if token := hfProv.GetToken(); token != "" {
		reqOpts = append(reqOpts, transport.HTTPRequestHeaders(map[string]string{
			"Authorization": fmt.Sprintf("Bearer %s", token),
		}))
	}

	return ht.Get(ctx, downloadURL, htCallback, reqOpts...)
}
