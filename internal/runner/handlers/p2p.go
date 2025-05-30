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

// P2PDownloadHandlerOption is an option for configuring P2PDownloadHandler
type P2PDownloadHandlerOption func(*P2PDownloadHandler)

// P2PDownloadHandlerWithLimiter sets a rate limiter for the handler
func P2PDownloadHandlerWithLimiter(limiter *types.RateLimiter) P2PDownloadHandlerOption {
	return func(h *P2PDownloadHandler) {
		h.limiter = limiter
	}
}

// P2PDownloadHandler downloads files using peer-to-peer transfers with fallback to origin
type P2PDownloadHandler struct {
	localBasePath string             // local base path
	localJobPath  string             // local job path -> localBasePath/localJobPath
	providerID    string             // ID of the origin provider to use
	repo          string             // Repository (e.g., "meta-llama/Llama-2-7b-hf")
	revision      string             // Repository revision (e.g., "main", "v1.0")
	remotePath    string             // remote file path within the repository
	peerSources   []PeerSource       // Available peer sources for this file
	blockStrategy string             // "peer_first", "provider_only", "parallel"
	limiter       *types.RateLimiter // Rate limiter for transfers
}

// PeerSource represents a peer that has blocks for a specific file
type PeerSource struct {
	AgentAddr       types.Address `json:"agent_addr"`       // Address to reach the peer
	AvailableBlocks []int         `json:"available_blocks"` // Which block indices this peer has
	Priority        int           `json:"priority"`         // Transfer priority (lower = higher priority)
}

// NewP2PDownloadHandler creates a new P2PDownloadHandler for downloading files with peer-to-peer
func NewP2PDownloadHandler(
	localBasePath, localJobPath,
	providerID, repo, revision, remotePath string,
	peerSources []PeerSource,
	blockStrategy string,
	opts ...P2PDownloadHandlerOption,
) *P2PDownloadHandler {
	h := &P2PDownloadHandler{
		localBasePath: localBasePath,
		localJobPath:  localJobPath,
		providerID:    providerID,
		repo:          repo,
		revision:      revision,
		remotePath:    remotePath,
		peerSources:   peerSources,
		blockStrategy: blockStrategy,
		limiter:       types.DefaultRateLimiter(),
	}

	for _, opt := range opts {
		opt(h)
	}

	return h
}

func (h *P2PDownloadHandler) Run(ctx context.Context, self *runner.Job) error {
	// Create destination directory
	filePath := filepath.Dir(h.remotePath)
	fileName := filepath.Base(h.remotePath)
	destPath := filepath.Join(h.localBasePath, h.localJobPath, filePath)

	if err := os.MkdirAll(destPath, 0755); err != nil {
		return err
	}

	destFile := filepath.Join(destPath, fileName)

	// Check if we have peer sources and use peer-first strategy
	if len(h.peerSources) > 0 && h.blockStrategy == "peer_first" {
		// Try to download from peers first
		if err := h.tryPeerDownload(ctx, self, destFile); err == nil {
			return nil // Success via peer transfer
		}
		// Continue to fallback on peer failure
	}

	// Fallback to origin provider or if no peers available
	return h.downloadFromOrigin(ctx, self, destFile)
}

// tryPeerDownload attempts to download the file from peer sources
func (h *P2PDownloadHandler) tryPeerDownload(ctx context.Context, self *runner.Job, destFile string) error {
	// For now, implement a simple approach that tries to get the complete file from the best peer
	// In a full implementation, this would:
	// 1. Get file metadata to determine block structure
	// 2. Query peers for available blocks
	// 3. Download blocks in parallel from multiple peers
	// 4. Assemble the final file

	if len(h.peerSources) == 0 {
		return fmt.Errorf("no peer sources available")
	}

	// Try the highest priority peer first
	bestPeer := h.peerSources[0]
	for _, peer := range h.peerSources {
		if peer.Priority < bestPeer.Priority {
			bestPeer = peer
		}
	}

	// Try to get the complete file from the best peer
	return h.downloadFromPeer(ctx, self, destFile, bestPeer)
}

// downloadFromPeer downloads a file from a specific peer
func (h *P2PDownloadHandler) downloadFromPeer(ctx context.Context, self *runner.Job, destFile string, peer PeerSource) error {
	// Create the peer file URL
	// In the peer API, files are served at /datasets/{repo}/files/{path}
	peerURL := fmt.Sprintf("%s/datasets/%s/files/%s", peer.AgentAddr.URL(), h.repo, h.remotePath)

	fileHandle, err := os.Create(destFile)
	if err != nil {
		return err
	}
	defer fileHandle.Close()

	// Create HTTP transport for peer communication
	ht := transport.NewHTTPTransfer(
		transport.HTTPWithClient(transport.DefaultHTTPClient()),
	)

	htCallback := func(resp *http.Response) error {
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("peer returned status %d", resp.StatusCode)
		}

		self.Tracker().SetTotal(resp.ContentLength)
		self.Tracker().SetCurrent(0)

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

	return ht.Get(ctx, peerURL, htCallback)
}

// downloadFromOrigin downloads the file from the origin provider
func (h *P2PDownloadHandler) downloadFromOrigin(ctx context.Context, self *runner.Job, destFile string) error {
	// Get the provider from the registry
	prov, err := provider.GetProvider(h.providerID)
	if err != nil {
		return fmt.Errorf("failed to get provider %s: %w", h.providerID, err)
	}

	fileHandle, err := os.Create(destFile)
	if err != nil {
		return err
	}
	defer fileHandle.Close()

	// Create HTTP transport for the actual download
	ht := transport.NewHTTPTransfer(
		transport.HTTPWithClient(transport.DefaultHTTPClient()),
	)

	var downloadURL string

	// Handle different provider types - cast to specific types to access methods
	switch prov.GetID() {
	default:
		// Try to cast to HFProvider for HuggingFace
		if hfProv, ok := prov.(*provider.HFProvider); ok {
			// Get file metadata to set up progress tracking
			fileInfo, err := hfProv.GetFileInfo(ctx, h.repo, h.revision, h.remotePath)
			if err != nil {
				return fmt.Errorf("failed to get file info: %w", err)
			}

			if fileInfo != nil {
				self.Tracker().SetTotal(int64(fileInfo.Size))
			}

			downloadURL = hfProv.BuildDownloadURL(h.repo, h.revision, h.remotePath)
		} else {
			return fmt.Errorf("unsupported provider type for P2P handler: %T", prov)
		}
	}

	htCallback := func(resp *http.Response) error {
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("origin provider returned status %d", resp.StatusCode)
		}

		// Set up progress tracking if not already set
		if self.Tracker().Total() == 0 {
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

	// Add headers to the request - try to get from HF provider
	reqOpts := []transport.HTTPRequestOption{}
	if hfProv, ok := prov.(*provider.HFProvider); ok {
		if token := hfProv.GetToken(); token != "" {
			authHeaders := map[string]string{
				"Authorization": fmt.Sprintf("Bearer %s", token),
			}
			reqOpts = append(reqOpts, transport.HTTPRequestHeaders(authHeaders))
		}
	}

	return ht.Get(ctx, downloadURL, htCallback, reqOpts...)
}
