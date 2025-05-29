package main

import (
	cli "syncbit/internal/cli"

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
	return client.ListJobs()
}

func (c *SubmitJobCmd) Run(cliRoot *CLI) error {
	client := cli.NewClient(cliRoot.ControllerURL)
	return client.SubmitJob(c.Repo, c.Files, c.LocalPath, c.ProviderID)
}

func (c *StatusCmd) Run(cliRoot *CLI) error {
	client := cli.NewClient(cliRoot.ControllerURL)
	return client.GetJob(c.JobID)
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
