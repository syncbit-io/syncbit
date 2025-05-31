package main

import (
	"syncbit/internal/agent"
	"syncbit/internal/controller"
	"syncbit/internal/core/types"

	"github.com/alecthomas/kong"
)

type AgentCmd struct {
	ConfigFile string `short:"c" long:"config" default:"${config_file}" help:"Path to config file"`
	Debug      bool   `short:"d" long:"debug" help:"Enable debug logging"`
}

type ControllerCmd struct {
	ConfigFile string `short:"c" long:"config" default:"${config_file}" help:"Path to config file"`
	Debug      bool   `short:"d" long:"debug" help:"Enable debug logging"`
}

type CLI struct {
	Version    kong.VersionFlag `short:"v" long:"version" help:"Print version and exit"`
	Agent      AgentCmd         `cmd:"agent" help:"Start the agent daemon"`
	Controller ControllerCmd    `cmd:"controller" help:"Start the controller daemon"`
}

func (a *AgentCmd) Run() error {
	// Create signal-aware context for the application
	ctx, cancel := types.DefaultSignalNotifySubContext()
	defer cancel()

	ag := agent.NewAgent(a.ConfigFile, a.Debug)
	return ag.Run(ctx)
}

func (c *ControllerCmd) Run() error {
	// Create signal-aware context for the application
	ctx, cancel := types.DefaultSignalNotifySubContext()
	defer cancel()

	ctrl := controller.NewController(c.ConfigFile, c.Debug)
	return ctrl.Run(ctx)
}

func main() {
	var cli CLI
	kctx := kong.Parse(
		&cli,
		kong.Vars{
			"version":     "0.1.0",
			"config_file": "config.yaml",
		},
		kong.Name("syncbitd"),
		kong.UsageOnError(),
		kong.ConfigureHelp(kong.HelpOptions{
			Compact: true,
		}),
	)
	if err := kctx.Run(&cli); err != nil {
		panic(err)
	}
}
