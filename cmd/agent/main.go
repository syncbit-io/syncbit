package main

import (
	"syncbit/internal/agent"
	"syncbit/internal/core/logger"
	"syncbit/internal/core/types"
)

func main() {
	ctx, cancel := types.DefaultSignalNotifySubContext()
	defer cancel()

	log := logger.NewLogger()
	log.Info("starting agent", "version", "0.0.1")

	cfg := agent.LoadConfig()
	agent := agent.NewAgent(cfg)
	err := agent.Run(ctx)
	if err != nil {
		log.Fatal("failed to run agent", "error", err)
	}
}
