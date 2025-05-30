package main

import (
	"fmt"

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
	ProviderID string   `short:"P" long:"provider" default:"hf-public" help:"Provider ID to use"`
	Revision   string   `short:"r" long:"revision" default:"main" help:"Git revision/branch to download"`
}

type StatusCmd struct {
	JobID string `arg:"" help:"Job ID to check status"`
}

type CLI struct {
	ControllerURL string       `short:"u" long:"url" default:"http://localhost:8080" help:"Controller URL"`
	List          ListCmd      `cmd:"list" help:"List all jobs"`
	Submit        SubmitJobCmd `cmd:"submit" help:"Submit a new download job"`
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
		fmt.Printf("Files: %v\n", job.Config.Files)
		fmt.Printf("Local Path: %s\n", job.Config.LocalPath)
		if job.Error != "" {
			fmt.Printf("Error: %s\n", job.Error)
		}
		fmt.Println("---")
	}

	return nil
}

func (c *SubmitJobCmd) Run(cliRoot *CLI) error {
	client := cli.NewClient(cliRoot.ControllerURL)

	config := types.JobConfig{
		ProviderID: c.ProviderID,
		Repo:       c.Repo,
		Revision:   c.Revision,
		Files:      c.Files,
		LocalPath:  c.LocalPath,
	}

	job, err := client.SubmitJob(config, types.JobHandlerHF)
	if err != nil {
		return err
	}

	fmt.Printf("Job submitted successfully!\n")
	fmt.Printf("Job ID: %s\n", job.ID)
	fmt.Printf("Status: %s\n", job.Status)

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
	fmt.Printf("Provider ID: %s\n", job.Config.ProviderID)
	fmt.Printf("Files: %v\n", job.Config.Files)
	fmt.Printf("Local Path: %s\n", job.Config.LocalPath)
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
