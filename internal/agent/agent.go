package agent

import (
	"context"
	"os"

	"syncbit/internal/core/logger"
	"syncbit/internal/runner"

	"github.com/joho/godotenv"
)

var log = logger.NewLogger(logger.WithName("agent"))

type AgentConfig struct {
	Debug  bool
	Server string
}

func LoadConfig() *AgentConfig {
	cfg := &AgentConfig{}
	if err := godotenv.Load(); err != nil {
		log.Warn("failed to read .env file; using default values", "error", err)
	}

	if debug, ok := os.LookupEnv("DEBUG"); ok {
		cfg.Debug = debug == "true"
	} else {
		cfg.Debug = false
	}

	if server, ok := os.LookupEnv("SERVER"); ok {
		cfg.Server = server
	} else {
		cfg.Server = "0.0.0.0:8080"
	}

	log.Info("loaded config", "debug", cfg.Debug, "server", cfg.Server)
	return cfg
}

type Agent struct {
	cfg *AgentConfig
}

func NewAgent(cfg *AgentConfig) *Agent {
	return &Agent{
		cfg: cfg,
	}
}

func (a *Agent) Run(ctx context.Context) error {
	log.Info("running agent", "server", a.cfg.Server)

	pool := runner.NewPool(ctx, "agent")
	pool.Submit(runner.NewJob("agent", func(ctx context.Context, job *runner.Job) error {
		log.Info("running job", "job", job.Name())
		return nil
	}))

	pool.Wait()

	return nil
}
