package controller

import (
	"net/http"

	"syncbit/internal/server"
)

// RegisterHandlers registers the handlers for the controller.
func (c *Controller) RegisterHandlers(registrar server.HandlerRegistrar) error {
	registrar.RegisterHandler(server.NewRoute(server.MethodGet, "/", c.handleRoot))
	registrar.RegisterHandler(server.NewRoute(server.MethodGet, "/health", c.handleHealth))
	return nil
}

// handleRoot is the handler for the root endpoint.
func (c *Controller) handleRoot(w http.ResponseWriter, r *http.Request) {
	server.NewResponse(w).Respond(
		server.WithJSON(server.JSON{"message": "Hello, World!"}),
	)
}

// handleHealth is the handler for the health endpoint.
func (c *Controller) handleHealth(w http.ResponseWriter, r *http.Request) {
	server.NewResponse(w).Respond(
		server.WithString("OK"),
	)
}
