package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"syncbit/internal/api"
	"syncbit/internal/cache"
	"syncbit/internal/config"
	"syncbit/internal/core/logger"
	"syncbit/internal/core/types"
	"syncbit/internal/provider"
	"syncbit/internal/transport"
)

type Agent struct {
	cfg           *types.AgentConfig
	providers     map[string]types.ProviderConfig
	logger        *logger.Logger
	httpClient    *transport.HTTPTransfer
	server        *api.Server
	cache         *cache.BlockCache
	
	assignments   map[string]*types.DatasetAssignment
	assignmentsMu sync.RWMutex
	
	transfers     map[string]*FileTransfer
	transfersMu   sync.RWMutex
	
	ctx           context.Context
	cancel        context.CancelFunc
}

type FileTransfer struct {
	Dataset   string
	Revision  string
	FilePath  string
	Provider  provider.Provider
	Progress  *types.FileStatus
	Cancel    context.CancelFunc
}

func NewAgent(configFile string, debug bool) (*Agent, error) {
	cfg, err := config.LoadConfig(configFile)
	if err != nil {
		// If config loading fails, create default config
		defaults := types.DefaultAgentConfig()
		cfg = &types.Config{
			Debug:     debug,
			Providers: make(map[string]types.ProviderConfig),
			Agent:     &defaults,
		}
	}

	if debug {
		cfg.Debug = true
	}

	if cfg.Agent == nil {
		defaults := types.DefaultAgentConfig()
		cfg.Agent = &defaults
	}

	if cfg.Agent.ID == "" {
		if hostname, err := os.Hostname(); err == nil {
			cfg.Agent.ID = fmt.Sprintf("agent-%s", hostname)
		} else {
			cfg.Agent.ID = fmt.Sprintf("agent-%d", time.Now().Unix())
		}
	}

	advertiseAddr := cfg.Agent.AdvertiseAddr
	if advertiseAddr == nil {
		advertiseAddr, _ = url.Parse("http://localhost:8081")
	}

	listenAddr := cfg.Agent.ListenAddr
	if listenAddr == nil {
		listenAddr, _ = url.Parse("http://0.0.0.0:8081")
	}

	httpClient := transport.NewHTTPTransfer()

	agentCache := cache.NewBlockCache(cfg.Agent.Storage.Cache.RAMLimit, cfg.Agent.Storage.BasePath)

	ctx, cancel := context.WithCancel(context.Background())

	loggerOpts := []logger.LoggerOption{logger.WithName("agent")}
	if cfg.Debug {
		loggerOpts = append(loggerOpts, logger.WithLevel(logger.LevelDebug))
	}

	agent := &Agent{
		cfg:         cfg.Agent,
		providers:   cfg.Providers,
		logger:      logger.NewLogger(loggerOpts...),
		httpClient:  httpClient,
		server:      api.NewServer(api.WithListen(listenAddr)),
		cache:       agentCache,
		assignments: make(map[string]*types.DatasetAssignment),
		transfers:   make(map[string]*FileTransfer),
		ctx:         ctx,
		cancel:      cancel,
	}

	if err := agent.RegisterHandlers(agent.server); err != nil {
		return nil, fmt.Errorf("failed to setup routes: %w", err)
	}

	if err := config.ValidateProviders(cfg.Providers); err != nil {
		return nil, fmt.Errorf("invalid provider configuration: %w", err)
	}

	// Initialize providers for this agent
	if err := provider.InitializeProviders(cfg.Providers); err != nil {
		return nil, fmt.Errorf("failed to initialize providers: %w", err)
	}

	return agent, nil
}

func (a *Agent) Start(ctx context.Context) error {
	a.logger.Info("Starting agent", 
		"id", a.cfg.ID,
		"listen", a.cfg.ListenAddr,
		"advertise", a.cfg.AdvertiseAddr,
		"controller", a.cfg.ControllerURL,
	)

	go a.heartbeatLoop(ctx)
	go a.reconcileLoop(ctx)
	go a.cleanupLoop(ctx)

	return a.server.Run(ctx)
}

func (a *Agent) Stop() error {
	a.cancel()
	
	a.transfersMu.Lock()
	for _, transfer := range a.transfers {
		transfer.Cancel()
	}
	a.transfersMu.Unlock()
	
	// Server will stop when context is cancelled
	return nil
}

func (a *Agent) heartbeatLoop(ctx context.Context) {
	heartbeatInterval := types.ParseDuration(a.cfg.HeartbeatInterval, 30*time.Second)
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			a.sendHeartbeat(ctx)
		}
	}
}

func (a *Agent) reconcileLoop(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			a.reconcileAssignments(ctx)
		}
	}
}

func (a *Agent) cleanupLoop(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			a.cleanupUnassignedDatasets()
		}
	}
}

func (a *Agent) sendHeartbeat(ctx context.Context) {
	state := a.buildAgentState()

	url := fmt.Sprintf("%s/agents/%s/heartbeat", a.cfg.ControllerURL, a.cfg.ID)
	
	req := transport.HTTPRequestHeaders(map[string]string{
		"X-Advertise-Addr": a.cfg.AdvertiseAddr.String(),
	})

	// Marshal state to JSON
	stateData, err := json.Marshal(state)
	if err != nil {
		a.logger.Error("Failed to marshal agent state", "error", err)
		return
	}

	err = a.httpClient.Post(ctx, url, func(resp *http.Response) error {
		defer resp.Body.Close()
		
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("heartbeat failed with status: %d", resp.StatusCode)
		}

		var result struct {
			Assignments []*types.DatasetAssignment `json:"assignments"`
		}
		
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			return err
		}

		a.updateAssignments(result.Assignments)
		
		return nil
	}, req, 
		transport.HTTPRequestBody(stateData),
		transport.HTTPRequestHeaders(map[string]string{
			"Content-Type": "application/json",
		}))

	if err != nil {
		a.logger.Error("Failed to send heartbeat", "error", err)
	}
}

func (a *Agent) buildAgentState() types.AgentState {
	cacheStats := a.getCacheStats()
	
	a.assignmentsMu.RLock()
	assignmentIDs := make([]string, 0, len(a.assignments))
	for id := range a.assignments {
		assignmentIDs = append(assignmentIDs, id)
	}
	a.assignmentsMu.RUnlock()

	localDatasets := a.getLocalDatasets()

	return types.AgentState{
		DiskUsed:      cacheStats.DiskUsage,
		DiskAvailable: cacheStats.DiskLimit - cacheStats.DiskUsage,
		Assignments:   assignmentIDs,
		LocalDatasets: localDatasets,
		LastUpdated:   time.Now(),
	}
}

func (a *Agent) getLocalDatasets() map[string]types.LocalDataset {
	datasets := make(map[string]types.LocalDataset)
	
	for _, datasetName := range a.listDatasets() {
		files := a.getDatasetFiles(datasetName)
		
		// Parse dataset name to get dataset and revision
		parts := strings.Split(datasetName, "@")
		if len(parts) != 2 {
			continue
		}
		
		localFiles := make(map[string]types.LocalFileInfo)
		var totalSize types.Bytes
		
		for path, fileInfo := range files {
			// Get block information from cache
			fileKey := a.getFileKey(parts[0], path)
			blocks := a.cache.GetAvailableBlocks(fileKey)
			
			localFiles[path] = types.LocalFileInfo{
				Path:        path,
				Size:        fileInfo.Size,
				IsComplete:  true,
				LastUpdated: fileInfo.ModTime,
				Blocks:      blocks,
			}
			totalSize += fileInfo.Size
		}
		datasets[datasetName] = types.LocalDataset{
			Name:         parts[0],
			Revision:     parts[1],
			Files:        localFiles,
			TotalSize:    totalSize,
			IsAssigned:   a.isDatasetAssigned(parts[0], parts[1]),
			LastAccessed: time.Now(),
		}
	}
	
	return datasets
}

func (a *Agent) isDatasetAssigned(name, revision string) bool {
	a.assignmentsMu.RLock()
	defer a.assignmentsMu.RUnlock()
	
	for _, assignment := range a.assignments {
		if assignment.Dataset == name && assignment.Revision == revision {
			return true
		}
	}
	return false
}

func (a *Agent) updateAssignments(assignments []*types.DatasetAssignment) {
	a.assignmentsMu.Lock()
	defer a.assignmentsMu.Unlock()

	newAssignments := make(map[string]*types.DatasetAssignment)
	for _, assignment := range assignments {
		newAssignments[assignment.ID] = assignment
	}

	for id := range a.assignments {
		if _, stillAssigned := newAssignments[id]; !stillAssigned {
			a.logger.Info("Assignment removed", "assignment_id", id)
		}
	}

	for id, assignment := range newAssignments {
		if _, exists := a.assignments[id]; !exists {
			a.logger.Info("New assignment received", 
				"assignment_id", id,
				"dataset", assignment.Dataset,
				"revision", assignment.Revision,
			)
		}
	}

	a.assignments = newAssignments
}

func (a *Agent) reconcileAssignments(ctx context.Context) {
	a.assignmentsMu.RLock()
	assignments := make([]*types.DatasetAssignment, 0, len(a.assignments))
	for _, assignment := range a.assignments {
		assignments = append(assignments, assignment)
	}
	a.assignmentsMu.RUnlock()

	if len(assignments) > 0 {
		a.logger.Info("Reconciling assignments", "count", len(assignments))
	}

	for _, assignment := range assignments {
		a.reconcileDataset(ctx, assignment)
	}
}

func (a *Agent) reconcileDataset(ctx context.Context, assignment *types.DatasetAssignment) {
	datasetKey := fmt.Sprintf("%s@%s", assignment.Dataset, assignment.Revision)
	
	a.logger.Debug("Reconciling dataset", "dataset", datasetKey)
	
	dataset, err := a.getDatasetMetadata(ctx, assignment.Dataset, assignment.Revision)
	if err != nil {
		a.logger.Error("Failed to get dataset metadata", 
			"dataset", datasetKey,
			"error", err,
		)
		return
	}

	a.logger.Debug("Retrieved dataset metadata", 
		"dataset", datasetKey,
		"files", len(dataset.Files),
	)

	assignment.Status.Files = make(map[string]*types.FileStatus)
	
	for _, file := range dataset.Files {
		a.logger.Debug("Processing file", "dataset", datasetKey, "file", file.Path, "size", file.Size)
		
		// Check if file is complete in block cache
		fileKey := a.getFileKey(assignment.Dataset, file.Path)
		
		if a.hasCompleteFile(fileKey, file.Size) {
			a.logger.Debug("File already complete", "file", file.Path)
			assignment.Status.Files[file.Path] = &types.FileStatus{
				Phase:       types.FilePhaseReady,
				BytesTotal:  file.Size,
				BytesStored: file.Size,
				LastUpdated: time.Now(),
			}
		} else {
			a.logger.Debug("File needs download", "file", file.Path)
			assignment.Status.Files[file.Path] = &types.FileStatus{
				Phase:       types.FilePhasePending,
				BytesTotal:  file.Size,
				BytesStored: 0,
				LastUpdated: time.Now(),
			}
			
			if !a.isTransferring(datasetKey, file.Path) {
				a.logger.Debug("Starting download", "file", file.Path)
				go a.downloadFile(ctx, assignment, dataset, file)
			}
		}
	}
	
	assignment.Status.UpdateProgress()
	
	if assignment.Status.Progress.FilesReady == assignment.Status.Progress.FilesTotal {
		assignment.Status.Phase = types.AssignmentPhaseReady
		assignment.Status.Message = "All files ready"
	} else {
		assignment.Status.Phase = types.AssignmentPhaseDownloading
		assignment.Status.Message = fmt.Sprintf("%d/%d files ready", 
			assignment.Status.Progress.FilesReady,
			assignment.Status.Progress.FilesTotal,
		)
	}
}

func (a *Agent) getDatasetMetadata(ctx context.Context, name, revision string) (*types.Dataset, error) {
	requestURL := a.cfg.ControllerURL.JoinPath("datasets", name, revision).String()
	
	a.logger.Debug("Fetching dataset metadata", "url", requestURL, "name", name, "revision", revision)
	
	var result struct {
		Dataset *types.Dataset `json:"dataset"`
	}
	err := a.httpClient.Get(ctx, requestURL, func(resp *http.Response) error {
		defer resp.Body.Close()
		
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("failed to get dataset: status %d", resp.StatusCode)
		}
		
		return json.NewDecoder(resp.Body).Decode(&result)
	})
	
	if err != nil {
		return nil, err
	}
	
	return result.Dataset, nil
}

func (a *Agent) isTransferring(dataset, filePath string) bool {
	a.transfersMu.RLock()
	defer a.transfersMu.RUnlock()
	
	key := fmt.Sprintf("%s/%s", dataset, filePath)
	_, exists := a.transfers[key]
	return exists
}

func (a *Agent) downloadFile(ctx context.Context, assignment *types.DatasetAssignment, dataset *types.Dataset, file types.DatasetFile) {
	transferKey := fmt.Sprintf("%s@%s/%s", dataset.Name, dataset.Revision, file.Path)
	
	sources, err := a.discoverSources(ctx, dataset, file)
	if err != nil {
		a.logger.Error("Failed to discover sources", 
			"file", transferKey,
			"error", err,
		)
		return
	}
	
	transferCtx, cancel := context.WithCancel(ctx)
	transfer := &FileTransfer{
		Dataset:  dataset.Name,
		Revision: dataset.Revision,
		FilePath: file.Path,
		Progress: &types.FileStatus{
			Phase:       types.FilePhaseDownloading,
			BytesTotal:  file.Size,
			BytesStored: 0,
			Sources:     sources,
			LastUpdated: time.Now(),
		},
		Cancel: cancel,
	}
	
	a.transfersMu.Lock()
	a.transfers[transferKey] = transfer
	a.transfersMu.Unlock()
	
	defer func() {
		a.transfersMu.Lock()
		delete(a.transfers, transferKey)
		a.transfersMu.Unlock()
	}()
	
	err = a.performTransfer(transferCtx, transfer, dataset, file)
	if err != nil {
		transfer.Progress.Phase = types.FilePhaseFailed
		transfer.Progress.Error = err.Error()
		a.logger.Error("File transfer failed", 
			"file", transferKey,
			"error", err,
		)
	} else {
		transfer.Progress.Phase = types.FilePhaseReady
		transfer.Progress.BytesStored = file.Size
		a.logger.Info("File transfer completed", "file", transferKey)
	}
}

func (a *Agent) discoverSources(ctx context.Context, dataset *types.Dataset, file types.DatasetFile) ([]string, error) {
	var sources []string
	
	// First, check for peers with complete files
	peers, err := a.getPeersWithFile(ctx, dataset.Name, dataset.Revision, file.Path)
	if err == nil && len(peers) > 0 {
		for _, peer := range peers {
			sources = append(sources, fmt.Sprintf("peer://%s", peer))
		}
		// If we have peers with complete files, prefer them over remote sources
		return sources, nil
	}
	
	// If no complete files available, check for block-level peers
	// Calculate total blocks needed for this file
	totalBlocks := int((file.Size + types.Bytes(cache.BlockSize) - 1) / types.Bytes(cache.BlockSize))
	
	// Check if any peers have blocks we can use
	hasPeerBlocks := false
	for blockIndex := 0; blockIndex < totalBlocks; blockIndex++ {
		blockPeers, err := a.getPeersWithBlock(ctx, dataset.Name, dataset.Revision, file.Path, blockIndex)
		if err == nil && len(blockPeers) > 0 {
			hasPeerBlocks = true
			break
		}
	}
	
	// If we have some peer blocks, add peer sources first
	if hasPeerBlocks {
		// Add a generic peer source that will handle block-level coordination
		sources = append(sources, "peer://block-level")
	}
	
	// Always add remote provider sources as fallback
	for _, providerCfg := range dataset.Sources {
		sources = append(sources, fmt.Sprintf("%s://%s", providerCfg.Type, providerCfg.ID))
	}
	
	return sources, nil
}

func (a *Agent) getPeersWithFile(ctx context.Context, dataset, revision, filePath string) ([]string, error) {
	requestURL := a.cfg.ControllerURL.JoinPath("files", dataset, revision, filePath, "peers").String()
	
	var result struct {
		Peers []struct {
			Address string `json:"address"`
		} `json:"peers"`
	}
	
	err := a.httpClient.Get(ctx, requestURL, func(resp *http.Response) error {
		defer resp.Body.Close()
		
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("failed to get peers: status %d", resp.StatusCode)
		}
		
		return json.NewDecoder(resp.Body).Decode(&result)
	})
	
	if err != nil {
		return nil, err
	}
	
	peers := make([]string, 0, len(result.Peers))
	for _, peer := range result.Peers {
		peers = append(peers, peer.Address)
	}
	
	return peers, nil
}

func (a *Agent) getPeersWithBlock(ctx context.Context, dataset, revision, filePath string, blockIndex int) ([]string, error) {
	requestURL := a.cfg.ControllerURL.JoinPath("files", dataset, revision, filePath, "blocks", fmt.Sprintf("%d", blockIndex), "peers").String()
	
	var result struct {
		Peers []struct {
			Address string `json:"address"`
		} `json:"peers"`
	}
	
	err := a.httpClient.Get(ctx, requestURL, func(resp *http.Response) error {
		defer resp.Body.Close()
		
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("failed to get block peers: status %d", resp.StatusCode)
		}
		
		return json.NewDecoder(resp.Body).Decode(&result)
	})
	
	if err != nil {
		return nil, err
	}
	
	var addresses []string
	for _, peer := range result.Peers {
		addresses = append(addresses, peer.Address)
	}
	
	return addresses, nil
}

func (a *Agent) downloadFromBlockLevelPeers(ctx context.Context, dataset *types.Dataset, file types.DatasetFile, transfer *FileTransfer) error {
	// Create file key for cache
	fileKey := a.getFileKey(dataset.Name, file.Path)
	
	// Initialize file in block cache
	a.cache.InitFile(fileKey, file.Size)
	
	// Calculate total blocks needed
	totalBlocks := int((file.Size + types.Bytes(cache.BlockSize) - 1) / types.Bytes(cache.BlockSize))
	
	a.logger.Debug("Starting block-level peer download", 
		"file", file.Path, 
		"totalBlocks", totalBlocks,
		"fileSize", file.Size)
	
	// Download blocks that are available from peers
	blocksDownloaded := 0
	for blockIndex := 0; blockIndex < totalBlocks; blockIndex++ {
		// Check if we already have this block
		if a.cache.HasBlock(fileKey, blockIndex) {
			blocksDownloaded++
			continue
		}
		
		// Find peers with this block
		peers, err := a.getPeersWithBlock(ctx, dataset.Name, dataset.Revision, file.Path, blockIndex)
		if err != nil || len(peers) == 0 {
			// No peers have this block, will need to get from remote provider
			continue
		}
		
		// Try to download from first available peer
		// TODO: Implement actual peer-to-peer block download
		a.logger.Debug("Would download block from peer", 
			"block", blockIndex, 
			"peer", peers[0])
		
		// For now, skip peer downloads (not implemented yet)
		// The fallback to remote provider will handle missing blocks
	}
	
	a.logger.Debug("Block-level peer download completed", 
		"blocksFromPeers", blocksDownloaded, 
		"totalBlocks", totalBlocks)
	
	// If we got all blocks from peers, we're done
	if blocksDownloaded == totalBlocks {
		return nil
	}
	
	// Otherwise, let remote provider handle the remaining blocks
	// Return an error to indicate this source didn't complete the file
	return fmt.Errorf("peer download incomplete: %d/%d blocks available from peers", blocksDownloaded, totalBlocks)
}

func (a *Agent) performTransfer(ctx context.Context, transfer *FileTransfer, dataset *types.Dataset, file types.DatasetFile) error {
	// Try downloading from discovered sources in order of preference
	sources, err := a.discoverSources(ctx, dataset, file)
	if err != nil {
		return fmt.Errorf("failed to discover sources: %w", err)
	}
	
	if len(sources) == 0 {
		return fmt.Errorf("no sources available for file %s", file.Path)
	}
	
	var lastErr error
	
	for _, source := range sources {
		a.logger.Debug("Attempting download from source", 
			"file", file.Path, 
			"source", source)
		
		// Parse source format: "type://id" or "peer://address"
		parts := strings.SplitN(source, "://", 2)
		if len(parts) != 2 {
			continue
		}
		
		sourceType := parts[0]
		sourceID := parts[1]
		
		switch sourceType {
		case "peer":
			if sourceID == "block-level" {
				err = a.downloadFromBlockLevelPeers(ctx, dataset, file, transfer)
			} else {
				err = a.downloadFromPeer(ctx, sourceID, dataset, file, transfer)
			}
		default:
			err = a.downloadFromProvider(ctx, sourceType, sourceID, dataset, file, transfer)
		}
		
		if err == nil {
			return nil // Success!
		}
		
		lastErr = err
		a.logger.Warn("Download failed from source", 
			"source", source, 
			"error", err)
	}
	
	return fmt.Errorf("all sources failed, last error: %w", lastErr)
}

func (a *Agent) downloadFromPeer(ctx context.Context, peerAddr string, dataset *types.Dataset, file types.DatasetFile, transfer *FileTransfer) error {
	// Parse peer address to URL
	peerURL, err := url.Parse(peerAddr)
	if err != nil {
		return fmt.Errorf("invalid peer address %s: %w", peerAddr, err)
	}
	
	// Use peer provider to download file
	peerProv, err := provider.NewPeerProvider(types.ProviderConfig{
		ID:   "peer-" + peerAddr,
		Type: "peer",
	}, types.DefaultTransferConfig())
	if err != nil {
		return fmt.Errorf("failed to create peer provider: %w", err)
	}
	
	peerProvider := peerProv.(*provider.PeerProvider)
	
	// Get file from peer (returns complete file as bytes)
	fileData, err := peerProvider.GetFileFromPeer(ctx, peerURL, dataset.Name, file.Path)
	if err != nil {
		return fmt.Errorf("failed to download from peer: %w", err)
	}
	
	// Store file data in cache
	err = a.storeFileData(dataset.Name, file.Path, fileData)
	if err != nil {
		return fmt.Errorf("failed to store file in cache: %w", err)
	}
	
	// Update progress
	transfer.Progress.BytesStored = types.Bytes(len(fileData))
	transfer.Progress.LastUpdated = time.Now()
	
	return nil
}

func (a *Agent) downloadFromProvider(ctx context.Context, providerType, providerID string, dataset *types.Dataset, file types.DatasetFile, transfer *FileTransfer) error {
	// Get provider from registry
	prov, err := provider.GetProvider(providerID)
	if err != nil {
		return fmt.Errorf("provider %s not found: %w", providerID, err)
	}
	
	// Use provider-specific download logic
	switch providerType {
	case "http":
		return a.downloadFromHTTP(ctx, prov, dataset, file, transfer)
	case "hf":
		return a.downloadFromHF(ctx, prov, dataset, file, transfer)
	case "s3":
		return a.downloadFromS3(ctx, prov, dataset, file, transfer)
	default:
		return fmt.Errorf("unsupported provider type: %s", providerType)
	}
}

func (a *Agent) downloadFromHTTP(ctx context.Context, prov provider.Provider, dataset *types.Dataset, file types.DatasetFile, transfer *FileTransfer) error {
	// Create file key for cache
	fileKey := a.getFileKey(dataset.Name, file.Path)
	
	// Initialize file in block cache
	a.cache.InitFile(fileKey, file.Size)
	
	// Create a BlockWriter for sequential writing
	blockWriter := a.cache.NewBlockWriter(fileKey, file.Size)
	defer blockWriter.Close()
	
	// Create ReaderWriter for the provider
	cacheRW := types.NewReaderWriter(types.RWWithIOWriter(blockWriter))
	
	// Use the dataset's Source field for the base URL for HTTP downloads
	if dataset.Source == "" {
		return fmt.Errorf("dataset source is required for HTTP downloads")
	}
	
	return prov.Download(ctx, dataset.Source, dataset.Revision, file.Path, cacheRW)
}

func (a *Agent) downloadFromHF(ctx context.Context, prov provider.Provider, dataset *types.Dataset, file types.DatasetFile, transfer *FileTransfer) error {
	// Create file key for cache
	fileKey := a.getFileKey(dataset.Name, file.Path)
	
	// Initialize file in block cache
	a.cache.InitFile(fileKey, file.Size)
	
	// Create a BlockWriter for sequential writing
	blockWriter := a.cache.NewBlockWriter(fileKey, file.Size)
	defer blockWriter.Close()
	
	// Create ReaderWriter for the provider
	cacheRW := types.NewReaderWriter(types.RWWithIOWriter(blockWriter))
	
	// Use the dataset's Source field for the HuggingFace repository name
	if dataset.Source == "" {
		return fmt.Errorf("dataset source is required for HuggingFace downloads")
	}
	
	return prov.Download(ctx, dataset.Source, dataset.Revision, file.Path, cacheRW)
}

func (a *Agent) downloadFromS3(ctx context.Context, prov provider.Provider, dataset *types.Dataset, file types.DatasetFile, transfer *FileTransfer) error {
	// TODO: Implement S3 download using existing handler logic
	return fmt.Errorf("S3 download not yet implemented")
}

func (a *Agent) cleanupUnassignedDatasets() {
	cacheStats := a.getCacheStats()
	
	if float64(cacheStats.DiskUsage) / float64(cacheStats.DiskLimit) < 0.8 {
		return
	}
	
	a.logger.Info("Starting cleanup of unassigned datasets", 
		"disk_usage", cacheStats.DiskUsage,
		"disk_limit", cacheStats.DiskLimit,
	)
	
	// Implementation for cleaning up unassigned datasets
}

// Helper methods for block cache integration

// getFileKey creates a unique key for a file
func (a *Agent) getFileKey(dataset, filepath string) string {
	return fmt.Sprintf("%s:%s", dataset, filepath)
}

// hasCompleteFile checks if a file is completely available in the block cache
func (a *Agent) hasCompleteFile(fileKey string, fileSize types.Bytes) bool {
	meta, exists := a.cache.GetFileMetadata(fileKey)
	if !exists {
		return false
	}
	return meta.IsComplete() && meta.TotalSize == fileSize
}

// storeFileData stores file data using the block cache
func (a *Agent) storeFileData(dataset, filepath string, data []byte) error {
	fileKey := a.getFileKey(dataset, filepath)
	a.cache.InitFile(fileKey, types.Bytes(len(data)))
	return a.cache.WriteAt(fileKey, 0, data)
}

// SimpleCacheStats provides basic cache statistics for agent state
type SimpleCacheStats struct {
	DiskUsage types.Bytes
	DiskLimit types.Bytes
}

// getCacheStats returns cache statistics (simplified)
func (a *Agent) getCacheStats() SimpleCacheStats {
	// For now, return basic info - could be enhanced to scan actual disk usage
	return SimpleCacheStats{
		DiskUsage: types.Bytes(0),      // TODO: scan actual disk usage
		DiskLimit: types.Bytes(1 << 40), // TODO: get from config - default 1TB
	}
}

// listDatasets returns a list of all datasets in cache
func (a *Agent) listDatasets() []string {
	// BlockCache doesn't track datasets separately
	// This would need to be implemented based on file keys
	return []string{}
}

// getDatasetFiles returns files for a dataset
func (a *Agent) getDatasetFiles(dataset string) map[string]types.FileInfo {
	// BlockCache doesn't group by dataset
	// This would need to scan all files and filter by dataset prefix
	return make(map[string]types.FileInfo)
}

// hasFileByPath checks if a file exists by dataset and path
func (a *Agent) hasFileByPath(dataset, path string) bool {
	fileKey := a.getFileKey(dataset, path)
	meta, exists := a.cache.GetFileMetadata(fileKey)
	return exists && meta.IsComplete()
}

// getFileByPath gets file data by dataset and path
func (a *Agent) getFileByPath(dataset, path string) ([]byte, error) {
	fileKey := a.getFileKey(dataset, path)
	return a.cache.ReadFile(fileKey)
}

// listFiles returns all files in cache (simplified)
func (a *Agent) listFiles() map[string]types.FileInfo {
	// TODO: Implement by scanning cache metadata
	return make(map[string]types.FileInfo)
}

// newCacheReader creates a reader for a file (simplified)
func (a *Agent) newCacheReader(dataset, path string, size types.Bytes) (io.ReadCloser, error) {
	fileKey := a.getFileKey(dataset, path)
	return a.cache.NewBlockReader(fileKey)
}

// getAgentFileAvailability returns file availability info
func (a *Agent) getAgentFileAvailability(dataset, path string) map[string]any {
	fileKey := a.getFileKey(dataset, path)
	meta, exists := a.cache.GetFileMetadata(fileKey)
	if !exists {
		return map[string]any{"available": false}
	}
	
	return map[string]any{
		"available":   meta.IsComplete(),
		"total_size":  meta.TotalSize,
		"block_count": meta.BlockCount,
	}
}