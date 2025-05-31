package api

import (
	"net/http"
)


// Route represents a registered API route
type Route struct {
	Method  string
	Path    string
	Handler http.HandlerFunc
}

func NewRoute(method, path string, handler http.HandlerFunc) Route {
	return Route{
		Method:  method,
		Path:    path,
		Handler: handler,
	}
}

// String returns the pattern format expected by Go 1.22+ ServeMux: "METHOD /path"
func (r *Route) String() string {
	if r.Method == "" {
		return r.Path
	}
	return r.Method + " " + r.Path
}

// HandlerRegistrar is an interface for packages to register their API handlers
type HandlerRegistrar interface {
	RegisterHandler(route Route) error
}

// EndpointHandler is an interface that packages can implement to register their handlers
type EndpointHandler interface {
	RegisterHandlers(registrar HandlerRegistrar) error
}
