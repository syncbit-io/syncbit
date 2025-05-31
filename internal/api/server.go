package api

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"syncbit/internal/core/logger"
	"syncbit/internal/core/types"
)

// mustParseURL is a helper for parsing URLs in default configs
func mustParseURL(rawURL string) *url.URL {
	u, err := url.Parse(rawURL)
	if err != nil {
		panic("invalid default URL: " + rawURL)
	}
	return u
}

type ServerOption func(*Server)

func WithLogger(logger *logger.Logger) ServerOption {
	return func(s *Server) {
		s.logger = logger
	}
}

func WithListen(listen *url.URL) ServerOption {
	return func(s *Server) {
		s.listen = listen
	}
}

// Server handles HTTP requests and route registration
type Server struct {
	logger     *logger.Logger
	httpServer *http.Server
	listen     *url.URL
	routes     map[string]http.HandlerFunc // Map of path+method to handler
}

// NewServer creates a new HTTP server
func NewServer(opts ...ServerOption) *Server {
	s := &Server{
		logger: logger.NewLogger(logger.WithName("server")),
		listen: mustParseURL("http://0.0.0.0:8080"),
		routes: make(map[string]http.HandlerFunc),
	}
	for _, opt := range opts {
		opt(s)
	}

	s.httpServer = &http.Server{
		Addr:    s.listen.Host, // This gives us the host:port format needed
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
	s.logger.Info("Starting server", "address", s.listen.String())
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
