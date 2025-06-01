package main

import (
	"fmt"

	cli "syncbit/internal/cli"
	"syncbit/internal/core/types"

	"github.com/alecthomas/kong"
)

type ListDatasetsCmd struct {
	// Add flags for list as needed
}

type CreateDatasetCmd struct {
	Name        string   `arg:"" help:"Dataset name (e.g., microsoft/DialoGPT-medium)"`
	Revision    string   `short:"r" long:"revision" default:"main" help:"Dataset revision/branch"`
	Replication int      `short:"n" long:"replication" default:"1" help:"Number of replicas"`
	Priority    int      `short:"p" long:"priority" default:"1" help:"Dataset priority"`
	Files       []string `short:"f" long:"files" help:"Files to include in dataset"`
	ProviderID  string   `short:"P" long:"provider" default:"hf-public" help:"Provider ID to use"`
	ProviderType string  `long:"provider-type" default:"hf" help:"Provider type (hf, http, s3)"`
}

type GetDatasetCmd struct {
	Name     string `arg:"" help:"Dataset name"`
	Revision string `short:"r" long:"revision" default:"main" help:"Dataset revision"`
}

type DeleteDatasetCmd struct {
	Name     string `arg:"" help:"Dataset name"`
	Revision string `short:"r" long:"revision" default:"main" help:"Dataset revision"`
}

type ListAgentsCmd struct {
	// Add flags for agents list as needed
}

type GetAgentCmd struct {
	AgentID string `arg:"" help:"Agent ID"`
}

type HealthCmd struct {
	// Check controller health
}

type CLI struct {
	ControllerURL   string            `short:"u" long:"url" default:"http://localhost:8080" help:"Controller URL"`
	ListDatasets    ListDatasetsCmd   `cmd:"datasets" help:"List all datasets"`
	CreateDataset   CreateDatasetCmd  `cmd:"create" help:"Create new dataset"`
	GetDataset      GetDatasetCmd     `cmd:"get" help:"Get dataset details"`
	DeleteDataset   DeleteDatasetCmd  `cmd:"delete" help:"Delete dataset"`
	ListAgents      ListAgentsCmd     `cmd:"agents" help:"List all agents"`
	GetAgent        GetAgentCmd       `cmd:"agent" help:"Get agent details"`
	Health          HealthCmd         `cmd:"health" help:"Check controller health"`
}

func (c *ListDatasetsCmd) Run(cliRoot *CLI) error {
	ctx, cancel := types.DefaultSignalNotifySubContext()
	defer cancel()
	client := cli.NewClient(cliRoot.ControllerURL)
	datasets, err := client.ListDatasets(ctx)
	if err != nil {
		return err
	}

	fmt.Printf("Found %d datasets:\n\n", len(datasets))
	for _, dataset := range datasets {
		fmt.Printf("Dataset: %s@%s\n", dataset.Name, dataset.Revision)
		fmt.Printf("Replication: %d\n", dataset.Replication)
		fmt.Printf("Priority: %d\n", dataset.Priority)
		fmt.Printf("Status: %s\n", dataset.Status.Phase)
		fmt.Printf("Ready Replicas: %d/%d\n", dataset.Status.ReadyReplicas, dataset.Status.TotalReplicas)
		if dataset.Status.Message != "" {
			fmt.Printf("Message: %s\n", dataset.Status.Message)
		}
		fmt.Printf("Files: %d\n", len(dataset.Files))
		for _, file := range dataset.Files {
			fmt.Printf("  - %s (%d bytes)\n", file.Path, file.Size)
		}
		fmt.Printf("Sources: %d\n", len(dataset.Sources))
		for _, source := range dataset.Sources {
			fmt.Printf("  - %s (%s)\n", source.ID, source.Type)
		}
		fmt.Printf("Created: %s\n", dataset.CreatedAt.Format("2006-01-02 15:04:05"))
		fmt.Println("---")
	}

	return nil
}

func (c *CreateDatasetCmd) Run(cliRoot *CLI) error {
	ctx, cancel := types.DefaultSignalNotifySubContext()
	defer cancel()
	client := cli.NewClient(cliRoot.ControllerURL)

	fmt.Printf("Creating dataset %s@%s with %d replicas...\n", c.Name, c.Revision, c.Replication)

	// Create provider config
	sources := []types.ProviderConfig{
		{
			ID:   c.ProviderID,
			Type: c.ProviderType,
		},
	}

	// Create dataset files
	var files []types.DatasetFile
	for _, filePath := range c.Files {
		files = append(files, types.DatasetFile{
			Path: filePath,
			Size: types.Bytes(0), // Size will be determined by provider
		})
	}

	dataset, err := client.CreateDataset(ctx, c.Name, c.Revision, c.Replication, sources, files)
	if err != nil {
		return err
	}

	fmt.Printf("✓ Dataset created successfully!\n")
	fmt.Printf("Name: %s@%s\n", dataset.Name, dataset.Revision)
	fmt.Printf("Replication: %d\n", dataset.Replication)
	fmt.Printf("Priority: %d\n", dataset.Priority)
	fmt.Printf("Files: %d\n", len(dataset.Files))
	fmt.Printf("Status: %s\n", dataset.Status.Phase)

	return nil
}

func (c *GetDatasetCmd) Run(cliRoot *CLI) error {
	ctx, cancel := types.DefaultSignalNotifySubContext()
	defer cancel()
	client := cli.NewClient(cliRoot.ControllerURL)
	
	dataset, err := client.GetDataset(ctx, c.Name, c.Revision)
	if err != nil {
		return err
	}

	fmt.Printf("Dataset Details:\n")
	fmt.Printf("Name: %s@%s\n", dataset.Name, dataset.Revision)
	fmt.Printf("Replication: %d\n", dataset.Replication)
	fmt.Printf("Priority: %d\n", dataset.Priority)
	fmt.Printf("Status: %s\n", dataset.Status.Phase)
	fmt.Printf("Ready Replicas: %d/%d\n", dataset.Status.ReadyReplicas, dataset.Status.TotalReplicas)
	if dataset.Status.Message != "" {
		fmt.Printf("Message: %s\n", dataset.Status.Message)
	}
	fmt.Printf("Created: %s\n", dataset.CreatedAt.Format("2006-01-02 15:04:05"))
	fmt.Printf("Updated: %s\n", dataset.UpdatedAt.Format("2006-01-02 15:04:05"))
	
	fmt.Printf("\nFiles (%d):\n", len(dataset.Files))
	for _, file := range dataset.Files {
		fmt.Printf("  - %s (%d bytes)\n", file.Path, file.Size)
		if file.Checksum != "" {
			fmt.Printf("    Checksum: %s\n", file.Checksum)
		}
	}
	
	fmt.Printf("\nSources (%d):\n", len(dataset.Sources))
	for _, source := range dataset.Sources {
		fmt.Printf("  - %s (%s)\n", source.ID, source.Type)
	}

	return nil
}

func (c *DeleteDatasetCmd) Run(cliRoot *CLI) error {
	ctx, cancel := types.DefaultSignalNotifySubContext()
	defer cancel()
	client := cli.NewClient(cliRoot.ControllerURL)
	
	fmt.Printf("Deleting dataset %s@%s...\n", c.Name, c.Revision)
	
	err := client.DeleteDataset(ctx, c.Name, c.Revision)
	if err != nil {
		return err
	}

	fmt.Printf("✓ Dataset %s@%s deleted successfully!\n", c.Name, c.Revision)
	return nil
}

func (c *ListAgentsCmd) Run(cliRoot *CLI) error {
	ctx, cancel := types.DefaultSignalNotifySubContext()
	defer cancel()
	client := cli.NewClient(cliRoot.ControllerURL)
	
	agents, err := client.ListAgents(ctx)
	if err != nil {
		return err
	}

	fmt.Printf("Found %d agents:\n\n", len(agents))
	for _, agent := range agents {
		fmt.Printf("Agent: %s\n", agent.ID)
		fmt.Printf("Address: %s\n", agent.AdvertiseAddr)
		fmt.Printf("Disk Used: %d bytes\n", agent.State.DiskUsed)
		fmt.Printf("Disk Available: %d bytes\n", agent.State.DiskAvailable)
		fmt.Printf("Assignments: %d\n", len(agent.State.Assignments))
		fmt.Printf("Local Datasets: %d\n", len(agent.State.LocalDatasets))
		fmt.Printf("Last Heartbeat: %s\n", agent.LastHeartbeat.Format("2006-01-02 15:04:05"))
		fmt.Println("---")
	}

	return nil
}

func (c *GetAgentCmd) Run(cliRoot *CLI) error {
	ctx, cancel := types.DefaultSignalNotifySubContext()
	defer cancel()
	client := cli.NewClient(cliRoot.ControllerURL)
	
	assignments, err := client.GetAgentAssignments(ctx, c.AgentID)
	if err != nil {
		return err
	}

	fmt.Printf("Agent %s Assignments:\n\n", c.AgentID)
	fmt.Printf("Found %d assignments:\n\n", len(assignments))
	for _, assignment := range assignments {
		fmt.Printf("Assignment: %s\n", assignment.ID)
		fmt.Printf("Dataset: %s@%s\n", assignment.Dataset, assignment.Revision)
		fmt.Printf("Priority: %d\n", assignment.Priority)
		fmt.Printf("Status: %s\n", assignment.Status.Phase)
		if assignment.Status.Message != "" {
			fmt.Printf("Message: %s\n", assignment.Status.Message)
		}
		fmt.Printf("Files: %d\n", len(assignment.Status.Files))
		fmt.Printf("Created: %s\n", assignment.CreatedAt.Format("2006-01-02 15:04:05"))
		fmt.Printf("Updated: %s\n", assignment.UpdatedAt.Format("2006-01-02 15:04:05"))
		fmt.Println("---")
	}

	return nil
}

func (c *HealthCmd) Run(cliRoot *CLI) error {
	ctx, cancel := types.DefaultSignalNotifySubContext()
	defer cancel()
	client := cli.NewClient(cliRoot.ControllerURL)
	
	health, err := client.GetControllerHealth(ctx)
	if err != nil {
		return err
	}

	fmt.Printf("Controller Health:\n")
	for key, value := range health {
		fmt.Printf("%s: %v\n", key, value)
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
		kong.Description("SyncBit - Distributed dataset synchronization and download tool"),
	)
	if err := kctx.Run(&cliRoot, &cliRoot); err != nil {
		panic(err)
	}
}