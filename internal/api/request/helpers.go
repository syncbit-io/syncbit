package request

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"syncbit/internal/transport"
)

// JSON represents a JSON request body
type JSON map[string]any

// WithJSON creates a request option that sets the request body to JSON
func WithJSON(data any) transport.HTTPRequestOption {
	return func(req *http.Request) {
		jsonData, err := json.Marshal(data)
		if err != nil {
			// Log error but don't fail the request setup
			// In a real implementation, you might want to log this
			return
		}
		req.Header.Set("Content-Type", "application/json")
		if len(jsonData) > 0 {
			req.Body = io.NopCloser(bytes.NewReader(jsonData))
			req.ContentLength = int64(len(jsonData))
		}
	}
}

// WithJSONMap creates a request option for a JSON map body
func WithJSONMap(data JSON) transport.HTTPRequestOption {
	return WithJSON(data)
}

// WithBody creates a request option that sets the request body
func WithBody(body []byte) transport.HTTPRequestOption {
	return func(req *http.Request) {
		req.Body = io.NopCloser(bytes.NewReader(body))
		req.ContentLength = int64(len(body))
	}
}

// WithString creates a request option that sets the request body to a string
func WithString(body string) transport.HTTPRequestOption {
	return func(req *http.Request) {
		req.Body = io.NopCloser(strings.NewReader(body))
		req.ContentLength = int64(len(body))
	}
}

// WithFormData creates a request option for form-encoded data
func WithFormData(data url.Values) transport.HTTPRequestOption {
	return func(req *http.Request) {
		encoded := data.Encode()
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.Body = io.NopCloser(strings.NewReader(encoded))
		req.ContentLength = int64(len(encoded))
	}
}

// WithHeader creates a request option that sets a single header
func WithHeader(key, value string) transport.HTTPRequestOption {
	return func(req *http.Request) {
		req.Header.Set(key, value)
	}
}

// WithHeaders creates a request option that sets multiple headers
func WithHeaders(headers map[string]string) transport.HTTPRequestOption {
	return func(req *http.Request) {
		for key, value := range headers {
			req.Header.Set(key, value)
		}
	}
}

// WithHeaderValues creates a request option that adds header values (allowing multiple values per key)
func WithHeaderValues(headers http.Header) transport.HTTPRequestOption {
	return func(req *http.Request) {
		for key, values := range headers {
			for _, value := range values {
				req.Header.Add(key, value)
			}
		}
	}
}

// WithBasicAuth creates a request option that sets Basic Authentication
func WithBasicAuth(username, password string) transport.HTTPRequestOption {
	return func(req *http.Request) {
		req.SetBasicAuth(username, password)
	}
}

// WithBearerToken creates a request option that sets Bearer token authentication
func WithBearerToken(token string) transport.HTTPRequestOption {
	return func(req *http.Request) {
		req.Header.Set("Authorization", "Bearer "+token)
	}
}

// WithUserAgent creates a request option that sets the User-Agent header
func WithUserAgent(userAgent string) transport.HTTPRequestOption {
	return func(req *http.Request) {
		req.Header.Set("User-Agent", userAgent)
	}
}

// WithAccept creates a request option that sets the Accept header
func WithAccept(contentType string) transport.HTTPRequestOption {
	return func(req *http.Request) {
		req.Header.Set("Accept", contentType)
	}
}

// WithContentType creates a request option that sets the Content-Type header
func WithContentType(contentType string) transport.HTTPRequestOption {
	return func(req *http.Request) {
		req.Header.Set("Content-Type", contentType)
	}
}

// WithQueryParam creates a request option that adds a query parameter
func WithQueryParam(key, value string) transport.HTTPRequestOption {
	return func(req *http.Request) {
		q := req.URL.Query()
		q.Add(key, value)
		req.URL.RawQuery = q.Encode()
	}
}

// WithQueryParams creates a request option that adds multiple query parameters
func WithQueryParams(params map[string]string) transport.HTTPRequestOption {
	return func(req *http.Request) {
		q := req.URL.Query()
		for key, value := range params {
			q.Add(key, value)
		}
		req.URL.RawQuery = q.Encode()
	}
}
