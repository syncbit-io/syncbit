package runner

import (
	"context"
	"fmt"
	"sync"
	"time"

	"syncbit/internal/cache"
	"syncbit/internal/core/logger"
	"syncbit/internal/core/types"
	"syncbit/internal/provider"
)

type ReconcilerConfig struct {
	SyncInterval    time.Duration
	Workers         int
	MaxRetries      int
	RetryBackoff    time.Duration
}

type Reconciler struct {
	logger       *logger.Logger
	cache        *cache.BlockCache
	providers    map[string]provider.Provider
	assignments  map[string]*types.DatasetAssignment
	mu           sync.RWMutex
	config       ReconcilerConfig
	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup
}

func NewReconciler(cache *cache.BlockCache, providers map[string]provider.Provider, config ReconcilerConfig) *Reconciler {
	ctx, cancel := context.WithCancel(context.Background())

	return &Reconciler{
		logger:      logger.NewLogger(logger.WithName("reconciler")),
		cache:       cache,
		providers:   providers,
		assignments: make(map[string]*types.DatasetAssignment),
		config:      config,
		ctx:         ctx,
		cancel:      cancel,
	}
}

func (r *Reconciler) Start() {
	r.logger.Info("starting reconciler", "workers", r.config.Workers, "interval", r.config.SyncInterval)

	// Start reconciliation workers
	r.wg.Add(r.config.Workers)
	for i := range r.config.Workers {
		go r.worker(i)
	}

	// Start periodic sync
	r.wg.Add(1)
	go r.periodicSync()
}

func (r *Reconciler) Stop() {
	r.logger.Info("stopping reconciler")
	r.cancel()
	r.wg.Wait()
}

func (r *Reconciler) UpdateAssignments(assignments []*types.DatasetAssignment) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Clear old assignments
	r.assignments = make(map[string]*types.DatasetAssignment)

	// Add new assignments
	for _, assignment := range assignments {
		r.assignments[assignment.ID] = assignment
		r.logger.Debug("updated assignment", "id", assignment.ID, "dataset", assignment.Dataset)
	}
}

func (r *Reconciler) GetAssignment(id string) (*types.DatasetAssignment, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	assignment, ok := r.assignments[id]
	return assignment, ok
}

func (r *Reconciler) GetAssignments() []*types.DatasetAssignment {
	r.mu.RLock()
	defer r.mu.RUnlock()

	assignments := make([]*types.DatasetAssignment, 0, len(r.assignments))
	for _, assignment := range r.assignments {
		assignments = append(assignments, assignment)
	}
	return assignments
}

func (r *Reconciler) periodicSync() {
	defer r.wg.Done()

	ticker := time.NewTicker(r.config.SyncInterval)
	defer ticker.Stop()

	// Initial sync
	r.syncAllAssignments()

	for {
		select {
		case <-r.ctx.Done():
			return
		case <-ticker.C:
			r.syncAllAssignments()
		}
	}
}

func (r *Reconciler) syncAllAssignments() {
	assignments := r.GetAssignments()
	r.logger.Debug("syncing assignments", "count", len(assignments))

	for _, assignment := range assignments {
		select {
		case <-r.ctx.Done():
			return
		default:
			r.enqueueAssignment(assignment)
		}
	}
}

func (r *Reconciler) enqueueAssignment(assignment *types.DatasetAssignment) {
	// Update assignment status
	assignment.Status.Phase = types.AssignmentPhaseReconciling
	assignment.Status.LastUpdated = time.Now()

	// TODO: Implement work queue for better scheduling
	// For now, reconcile directly
	go r.reconcileAssignment(assignment)
}

func (r *Reconciler) worker(id int) {
	defer r.wg.Done()
	r.logger.Debug("worker started", "id", id)

	// TODO: Implement work queue
	// For now, workers will be triggered by enqueueAssignment
	<-r.ctx.Done()

	r.logger.Debug("worker stopped", "id", id)
}

func (r *Reconciler) reconcileAssignment(assignment *types.DatasetAssignment) {
	r.logger.Debug("reconciling assignment", "id", assignment.ID, "dataset", assignment.Dataset)

	// Get dataset details (this would come from controller API)
	dataset := r.getDatasetDetails(assignment.Dataset, assignment.Revision)
	if dataset == nil {
		r.updateAssignmentStatus(assignment, types.AssignmentPhaseFailed, "dataset not found")
		return
	}

	// Reconcile each file
	allReady := true
	for _, file := range dataset.Files {
		if err := r.reconcileFile(assignment, dataset, file); err != nil {
			r.logger.Error("failed to reconcile file", "file", file.Path, "error", err)
			allReady = false
		}
	}

	// Update assignment status
	if allReady {
		r.updateAssignmentStatus(assignment, types.AssignmentPhaseReady, "all files ready")
	} else {
		r.updateAssignmentStatus(assignment, types.AssignmentPhaseDownloading, "downloading files")
	}
}

func (r *Reconciler) reconcileFile(assignment *types.DatasetAssignment, dataset *types.Dataset, file types.DatasetFile) error {
	// Check if file exists in cache
	fileKey := fmt.Sprintf("%s:%s", dataset.Name, file.Path)
	meta, exists := r.cache.GetFileMetadata(fileKey)
	isComplete := exists && meta.IsComplete() && meta.TotalSize == file.Size

	if isComplete {
		// Verify file integrity if checksum is available
		if file.Checksum != "" {
			// TODO: Implement checksum verification
			r.logger.Debug("file exists in cache", "file", file.Path)
		}
		r.updateFileStatus(assignment, file.Path, types.FilePhaseReady, nil)
		return nil
	}

	// File doesn't exist - need to download
	r.updateFileStatus(assignment, file.Path, types.FilePhaseDiscovering, nil)

	// Discover sources (peers and upstream)
	sources := r.discoverSources(dataset, file)
	if len(sources) == 0 {
		return fmt.Errorf("no sources available for file %s", file.Path)
	}

	// Try sources in order
	r.updateFileStatus(assignment, file.Path, types.FilePhaseDownloading, nil)

	for _, source := range sources {
		err := r.downloadFromSource(dataset, file, source)
		if err == nil {
			r.updateFileStatus(assignment, file.Path, types.FilePhaseReady, nil)
			return nil
		}
		r.logger.Warn("failed to download from source", "source", source, "error", err)
	}

	r.updateFileStatus(assignment, file.Path, types.FilePhaseFailed, fmt.Errorf("all sources failed"))
	return fmt.Errorf("failed to download file %s from all sources", file.Path)
}

func (r *Reconciler) discoverSources(dataset *types.Dataset, file types.DatasetFile) []string {
	var sources []string

	// TODO: Query controller for peers with this file
	// For now, just use configured upstream sources

	// Add upstream sources
	for _, providerConfig := range dataset.Sources {
		sources = append(sources, providerConfig.Type)
	}

	return sources
}

func (r *Reconciler) downloadFromSource(dataset *types.Dataset, file types.DatasetFile, source string) error {
	// Get provider
	_, ok := r.providers[source]
	if !ok {
		return fmt.Errorf("provider %s not found", source)
	}

	// TODO: Implement proper provider download integration with BlockCache
	// For now, this is a placeholder that would need to be implemented
	// based on the specific provider API and how it integrates with the new BlockCache
	return fmt.Errorf("provider download integration with BlockCache not yet implemented")
}

func (r *Reconciler) getDatasetDetails(name, revision string) *types.Dataset {
	// TODO: Fetch from controller API
	// For now, return a mock dataset
	return nil
}

func (r *Reconciler) updateAssignmentStatus(assignment *types.DatasetAssignment, phase types.AssignmentPhase, message string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	assignment.Status.Phase = phase
	assignment.Status.Message = message
	assignment.Status.LastUpdated = time.Now()
	assignment.UpdatedAt = time.Now()

	// Update progress
	assignment.Status.UpdateProgress()

	r.logger.Debug("updated assignment status",
		"id", assignment.ID,
		"phase", phase,
		"ready", assignment.Status.Progress.FilesReady,
		"total", assignment.Status.Progress.FilesTotal)
}

func (r *Reconciler) updateFileStatus(assignment *types.DatasetAssignment, filePath string, phase types.FilePhase, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if assignment.Status.Files == nil {
		assignment.Status.Files = make(map[string]*types.FileStatus)
	}

	status, ok := assignment.Status.Files[filePath]
	if !ok {
		status = &types.FileStatus{}
		assignment.Status.Files[filePath] = status
	}

	status.Phase = phase
	status.LastUpdated = time.Now()
	if err != nil {
		status.Error = err.Error()
	} else {
		status.Error = ""
	}
}
