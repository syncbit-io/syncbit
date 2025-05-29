package api

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"syncbit/internal/core/logger"
	"syncbit/internal/core/types"
)

type ServerOption func(*Server)

func WithLogger(logger *logger.Logger) ServerOption {
	return func(s *Server) {
		s.logger = logger
	}
}

func WithListen(listen types.Address) ServerOption {
	return func(s *Server) {
		s.listen = listen
	}
}

// Server handles HTTP requests and route registration
type Server struct {
	logger     *logger.Logger
	httpServer *http.Server
	listen     types.Address
	routes     map[string]http.HandlerFunc // Map of path+method to handler
}

// NewServer creates a new HTTP server
func NewServer(opts ...ServerOption) *Server {
	s := &Server{
		logger: logger.NewLogger(logger.WithName("server")),
		listen: types.NewAddress("0.0.0.0", 8080),
		routes: make(map[string]http.HandlerFunc),
	}
	for _, opt := range opts {
		opt(s)
	}

	s.httpServer = &http.Server{
		Addr:    s.listen.String(),
		Handler: http.NewServeMux(),
	}

	return s
}

// RegisterHandler implements the HandlerRegistrar interface
func (s *Server) RegisterHandler(route Route) error {
	s.routes[route.String()] = route.Handler
	s.logger.Debug("Registered handler", "route", route.String())
	return nil
}

// Run begins the server and blocks until context is cancelled
func (s *Server) Run(ctx context.Context) error {
	// Register all custom handlers from the routes map
	for pattern, handler := range s.routes {
		s.httpServer.Handler.(*http.ServeMux).HandleFunc(pattern, handler)
	}

	// Start server in a goroutine so we can wait for ctx.Done
	s.logger.Info("Starting server", "address", s.listen.URL())
	errCh := make(chan error, 1)
	go func() {
		errCh <- s.httpServer.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		s.logger.Info("Stopping server")
		shutdownCtx, cancel := types.NewTimeoutSubContext(ctx, 5*time.Second)
		defer cancel()
		if err := s.httpServer.Shutdown(shutdownCtx); err != nil {
			s.logger.Error("Failed to shutdown server", "error", err)
			return fmt.Errorf("failed to shutdown server: %w", err)
		}
	case err := <-errCh:
		if err != http.ErrServerClosed {
			s.logger.Error("Server error", "error", err)
			return fmt.Errorf("server error: %w", err)
		}
	}

	return nil
}
