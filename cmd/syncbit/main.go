package main

import (
	"fmt"
	"time"

	cli "syncbit/internal/cli"
	"syncbit/internal/core/types"

	"github.com/alecthomas/kong"
)

type ListCmd struct {
	// Add flags for list as needed
}

type SubmitJobCmd struct {
	Repo       string   `arg:"" help:"HuggingFace repository (e.g., microsoft/DialoGPT-medium)"`
	Files      []string `short:"f" long:"files" help:"Files to download" required:""`
	LocalPath  string   `short:"p" long:"local-path" help:"Local destination path" required:""`
	ProviderID string   `short:"P" long:"provider" default:"hf-main" help:"Provider ID to use"`
	Revision   string   `short:"r" long:"revision" default:"main" help:"Git revision/branch to download"`

	// Distribution options
	DistStrategy string   `short:"d" long:"distribution" default:"single" help:"Distribution strategy: single, all, count, specific"`
	TargetAgents []string `long:"target-agents" help:"Specific agent IDs to target (for 'specific' strategy)"`
	TargetCount  int      `long:"target-count" help:"Number of agents to target (for 'count' strategy)"`
}

type StatusCmd struct {
	JobID string `arg:"" help:"Job ID to check status"`
}

type CLI struct {
	ControllerURL string       `short:"u" long:"url" default:"http://localhost:8080" help:"Controller URL"`
	List          ListCmd      `cmd:"list" help:"List all jobs"`
	Submit        SubmitJobCmd `cmd:"submit" help:"Submit new download jobs"`
	Status        StatusCmd    `cmd:"status" help:"Check job status"`
	// ... other subcommands
}

func (c *ListCmd) Run(cliRoot *CLI) error {
	client := cli.NewClient(cliRoot.ControllerURL)
	jobs, err := client.ListJobs()
	if err != nil {
		return err
	}

	fmt.Printf("Found %d jobs:\n\n", len(jobs))
	for _, job := range jobs {
		fmt.Printf("Job ID: %s\n", job.ID)
		fmt.Printf("Handler: %s\n", job.Handler)
		fmt.Printf("Status: %s\n", job.Status)
		fmt.Printf("Repository: %s\n", job.Config.Repo)
		fmt.Printf("File: %s\n", job.Config.FilePath)
		fmt.Printf("Local Path: %s\n", job.Config.LocalPath)
		fmt.Printf("Provider: %s\n", job.Config.ProviderSource.ProviderID)
		fmt.Printf("Distribution: %s\n", job.Config.Distribution.Strategy)
		if job.Error != "" {
			fmt.Printf("Error: %s\n", job.Error)
		}
		fmt.Println("---")
	}

	return nil
}

func (c *SubmitJobCmd) Run(cliRoot *CLI) error {
	client := cli.NewClient(cliRoot.ControllerURL)

	fmt.Printf("Submitting %d file download jobs with %s distribution...\n", len(c.Files), c.DistStrategy)

	// Create distribution request
	distribution := types.DistributionRequest{
		Strategy:     c.DistStrategy,
		TargetAgents: c.TargetAgents,
		TargetCount:  c.TargetCount,
	}

	var submittedJobs []*types.Job
	for i, filePath := range c.Files {
		// Create job config for this file
		config := types.JobConfig{
			Repo:      c.Repo,
			Revision:  c.Revision,
			FilePath:  filePath,
			LocalPath: c.LocalPath,
			ProviderSource: types.ProviderSource{
				ProviderID: c.ProviderID,
			},
			Distribution: distribution,
		}

		// Generate unique job ID
		jobID := fmt.Sprintf("download-%s-%s-%d-%d", c.Repo, filePath, time.Now().Unix(), i)

		job, err := client.SubmitJob(jobID, config, types.JobHandlerDownload)
		if err != nil {
			fmt.Printf("Failed to submit job for file %s: %v\n", filePath, err)
			continue
		}

		submittedJobs = append(submittedJobs, job)
		fmt.Printf("✓ Job submitted for file: %s (ID: %s)\n", filePath, job.ID)
	}

	fmt.Printf("\nSuccessfully submitted %d jobs with %s distribution!\n", len(submittedJobs), c.DistStrategy)
	return nil
}

func (c *StatusCmd) Run(cliRoot *CLI) error {
	client := cli.NewClient(cliRoot.ControllerURL)
	job, err := client.GetJob(c.JobID)
	if err != nil {
		return err
	}

	fmt.Printf("Job Details:\n")
	fmt.Printf("ID: %s\n", job.ID)
	fmt.Printf("Handler: %s\n", job.Handler)
	fmt.Printf("Status: %s\n", job.Status)
	fmt.Printf("Repository: %s\n", job.Config.Repo)
	fmt.Printf("Revision: %s\n", job.Config.Revision)
	fmt.Printf("File: %s\n", job.Config.FilePath)
	fmt.Printf("Local Path: %s\n", job.Config.LocalPath)
	fmt.Printf("Provider: %s\n", job.Config.ProviderSource.ProviderID)
	if job.Config.ProviderSource.PeerAddr != (types.Address{}) {
		fmt.Printf("Peer Address: %s\n", job.Config.ProviderSource.PeerAddr.URL())
	}
	fmt.Printf("Distribution Strategy: %s\n", job.Config.Distribution.Strategy)
	if job.Error != "" {
		fmt.Printf("Error: %s\n", job.Error)
	}

	return nil
}

func main() {
	var cliRoot CLI
	kctx := kong.Parse(
		&cliRoot,
		kong.Vars{
			"version": "0.1.0",
		},
		kong.Name("syncbit"),
		kong.UsageOnError(),
		kong.ConfigureHelp(kong.HelpOptions{
			Compact: true,
		}),
		kong.Description("SyncBit - Distributed file synchronization and download tool"),
	)
	if err := kctx.Run(&cliRoot, &cliRoot); err != nil {
		panic(err)
	}
}
