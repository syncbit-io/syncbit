package main

import (
	"syncbit/internal/controller"
	"syncbit/internal/core/logger"
	"syncbit/internal/core/types"
)

func main() {
	ctx, cancel := types.DefaultSignalNotifySubContext()
	defer cancel()

	logger.SetDefaultLevel(logger.LevelDebug)

	log := logger.NewLogger(logger.WithName("controller"))
	log.Info("Starting controller")

	controller := controller.NewController(
		controller.WithLogger(log),
	)
	err := controller.Run(ctx)
	if err != nil {
		log.Fatal("Failed to run controller", "error", err)
	}
}
