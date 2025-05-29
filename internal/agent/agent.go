package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"syncbit/internal/config"
	"syncbit/internal/core/logger"
	"syncbit/internal/provider"
	"syncbit/internal/runner"
	"syncbit/internal/runner/handlers"

	"gopkg.in/yaml.v3"
)

// Job represents a job from the controller
type Job struct {
	ID      string    `json:"id"`
	Type    string    `json:"type"`
	Handler string    `json:"handler"`
	Config  JobConfig `json:"config"`
	Status  string    `json:"status"`
	Error   string    `json:"error,omitempty"`
}

// JobConfig represents the configuration for a download job
type JobConfig struct {
	ProviderID string   `json:"provider_id"`
	Repo       string   `json:"repo"`
	Revision   string   `json:"revision"`
	Files      []string `json:"files"`
	LocalPath  string   `json:"local_path"`
}

type Config struct {
	AgentURL      string `yaml:"agent_url"`
	ControllerURL string `yaml:"controller_url"`
	Debug         bool   `yaml:"debug"`
}

type Agent struct {
	cfg        *Config
	daemonCfg  *config.DaemonConfig
	log        *logger.Logger
	httpClient *http.Client
}

type AgentOption func(*Agent)

func WithLogger(log *logger.Logger) AgentOption {
	return func(a *Agent) {
		a.log = log
	}
}

func NewAgent(configFile string, debug bool, opts ...AgentOption) *Agent {
	// Load both agent and daemon configuration
	cfg := &Config{}
	daemonCfg := &config.DaemonConfig{}

	f, err := os.Open(configFile)
	if err == nil {
		defer f.Close()
		var fullConfig struct {
			Agent  Config              `yaml:"agent"`
			Daemon config.DaemonConfig `yaml:"daemon"`
		}
		if err := yaml.NewDecoder(f).Decode(&fullConfig); err == nil {
			cfg = &fullConfig.Agent
			daemonCfg = &fullConfig.Daemon
		}
	}

	if debug {
		cfg.Debug = true
		daemonCfg.Debug = true
	}

	// Use daemon controller URL if agent doesn't have one
	if cfg.ControllerURL == "" && daemonCfg.ControllerURL != "" {
		cfg.ControllerURL = daemonCfg.ControllerURL
	}

	a := &Agent{
		cfg:       cfg,
		daemonCfg: daemonCfg,
		log:       logger.NewLogger(logger.WithName("agent")),
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}

	for _, opt := range opts {
		opt(a)
	}

	return a
}

func (a *Agent) Run(ctx context.Context) error {
	a.log.Info("Starting agent", "controller", a.cfg.ControllerURL)

	// Initialize providers from daemon config
	if err := provider.InitFromDaemonConfig(*a.daemonCfg); err != nil {
		a.log.Error("Failed to initialize providers", "error", err)
		return fmt.Errorf("failed to initialize providers: %w", err)
	}
	a.log.Info("Initialized providers", "providers", provider.ListProviders())

	// Start job polling loop
	for {
		select {
		case <-ctx.Done():
			a.log.Info("Agent shutdown requested")
			return nil
		default:
			if err := a.pollAndExecuteJob(ctx); err != nil {
				a.log.Error("Error processing job", "error", err)
				// Continue polling even if job execution fails
			}
		}

		// Brief pause before next poll to avoid overwhelming the controller
		time.Sleep(2 * time.Second)
	}
}

func (a *Agent) pollAndExecuteJob(ctx context.Context) error {
	// Poll controller for next job
	job, err := a.getNextJob(ctx)
	if err != nil {
		return fmt.Errorf("failed to get next job: %w", err)
	}

	if job == nil {
		// No jobs available, this is normal
		return nil
	}

	a.log.Info("Received job", "job_id", job.ID, "type", job.Type, "handler", job.Handler)

	// Execute the job
	if err := a.executeJob(ctx, job); err != nil {
		a.log.Error("Job execution failed", "job_id", job.ID, "error", err)
		// Update job status to failed
		a.updateJobStatus(job.ID, "failed", err.Error())
		return err
	}

	a.log.Info("Job completed successfully", "job_id", job.ID)
	// Update job status to completed
	a.updateJobStatus(job.ID, "completed", "")
	return nil
}

func (a *Agent) getNextJob(ctx context.Context) (*Job, error) {
	url := fmt.Sprintf("%s/jobs/next", a.cfg.ControllerURL)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusRequestTimeout {
		// No jobs available - this is normal
		return nil, nil
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var response struct {
		Job *Job `json:"job"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, err
	}

	return response.Job, nil
}

func (a *Agent) updateJobStatus(jobID, status, errorMsg string) {
	statusUpdate := struct {
		Status string `json:"status"`
		Error  string `json:"error,omitempty"`
	}{
		Status: status,
		Error:  errorMsg,
	}

	jsonData, err := json.Marshal(statusUpdate)
	if err != nil {
		a.log.Error("Failed to marshal status update", "error", err)
		return
	}

	url := fmt.Sprintf("%s/jobs/%s/status", a.cfg.ControllerURL, jobID)
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		a.log.Error("Failed to create status update request", "error", err)
		return
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := a.httpClient.Do(req)
	if err != nil {
		a.log.Error("Failed to update job status", "error", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		a.log.Error("Failed to update job status", "status_code", resp.StatusCode)
	}
}

func (a *Agent) executeJob(ctx context.Context, job *Job) error {
	switch job.Handler {
	case "hf":
		return a.executeHFJob(ctx, job)
	default:
		return fmt.Errorf("unsupported job handler: %s", job.Handler)
	}
}

func (a *Agent) executeHFJob(ctx context.Context, job *Job) error {
	// Create a job pool for this download
	pool := runner.NewPool(ctx, fmt.Sprintf("job-%s", job.ID))

	// Create download tasks for each file
	// Keep track of job-to-filename mapping for better error reporting
	jobFileMap := make(map[string]string)

	for _, fileName := range job.Config.Files {
		// Create handler for this file
		handler := handlers.NewHFDownloadHandler(
			job.Config.LocalPath,  // localBasePath
			"",                    // localJobPath (empty for root)
			job.Config.ProviderID, // providerID
			job.Config.Repo,       // repo
			job.Config.Revision,   // revision
			fileName,              // remotePath
		)

		// Create a runner job for this file
		fileJobName := fmt.Sprintf("%s-%s", job.ID, filepath.Base(fileName))
		fileJob := runner.NewJob(
			fileJobName,
			handler.Run,
		)

		// Map job name to original filename for error reporting
		jobFileMap[fileJobName] = fileName

		// Submit the file download job
		if err := pool.Submit(fileJob); err != nil {
			return fmt.Errorf("failed to submit file job for %s: %w", fileName, err)
		}

		a.log.Debug("Submitted file download job", "file", fileName, "job_id", job.ID)
	}

	// Wait for all file downloads to complete
	completedCount := 0
	totalFiles := len(job.Config.Files)

	for completedCount < totalFiles {
		fileJob, err := pool.Wait()
		if err != nil {
			return fmt.Errorf("error waiting for file download: %w", err)
		}

		completedCount++
		if fileJob.Tracker().IsFailed() {
			// Use original filename instead of job name for error reporting
			originalFileName := jobFileMap[fileJob.Name()]
			if originalFileName == "" {
				originalFileName = fileJob.Name() // fallback to job name if mapping not found
			}
			return fmt.Errorf("file download failed: %s", originalFileName)
		}

		a.log.Debug("File download completed", "file_job", fileJob.Name(), "progress", fmt.Sprintf("%d/%d", completedCount, totalFiles))
	}

	a.log.Info("All files downloaded successfully", "job_id", job.ID, "files", totalFiles)
	return nil
}
