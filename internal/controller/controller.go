package controller

import (
	"context"

	"syncbit/internal/core/logger"
	"syncbit/internal/server"
)

type ControllerOption func(*Controller)

// WithLogger sets the logger for the controller.
func WithLogger(logger *logger.Logger) ControllerOption {
	return func(c *Controller) {
		c.logger = logger
	}
}

// WithServer sets the server for the controller.
func WithServer(server *server.Server) ControllerOption {
	return func(c *Controller) {
		c.server = server
	}
}

// Controller is the main controller for the application.
type Controller struct {
	logger *logger.Logger
	server *server.Server
}

// NewController creates a new controller with the given options.
func NewController(opts ...ControllerOption) *Controller {
	c := &Controller{
		logger: logger.NewLogger(logger.WithName("controller")),
		server: server.NewServer(),
	}

	for _, opt := range opts {
		opt(c)
	}

	return c
}

// Run starts the controller and blocks until the server is stopped
func (c *Controller) Run(ctx context.Context) error {
	if err := c.RegisterHandlers(c.server); err != nil {
		return err
	}

	return c.server.Run(ctx)
}
