package response

import (
	"encoding/json"
	"net/http"
)

type JSON map[string]any

type Response struct {
	http.ResponseWriter
	status int
	body   []byte
}

type ResponseOption func(*Response)

func Respond(w http.ResponseWriter, opts ...ResponseOption) {
	r := &Response{
		ResponseWriter: w,
		status:         http.StatusOK,
		body:           nil,
	}
	for _, opt := range opts {
		opt(r)
	}
	r.WriteHeader(r.status)
	if r.body != nil {
		r.Write(r.body)
	}
}

func WithHeader(key, value string) ResponseOption {
	return func(r *Response) {
		r.Header().Add(key, value)
	}
}

func WithHeaders(headers http.Header) ResponseOption {
	return func(r *Response) {
		for key, values := range headers {
			for _, value := range values {
				r.Header().Add(key, value)
			}
		}
	}
}

func WithStatus(status int) ResponseOption {
	return func(r *Response) {
		r.status = status
	}
}

func WithBody(body []byte) ResponseOption {
	return func(r *Response) {
		r.body = body
	}
}

func WithString(s string) ResponseOption {
	return func(r *Response) {
		r.body = []byte(s)
	}
}

func WithStringStatus(s string, status int) ResponseOption {
	return func(r *Response) {
		r.body = []byte(s)
		r.status = status
	}
}

func WithStringError(err error) ResponseOption {
	return func(r *Response) {
		r.body = []byte(err.Error())
		r.status = http.StatusInternalServerError
	}
}

func WithJSON(v JSON) ResponseOption {
	return func(r *Response) {
		jsonWrapper(r, v, http.StatusOK)
	}
}

func WithJSONStatus(v JSON, status int) ResponseOption {
	return func(r *Response) {
		jsonWrapper(r, v, status)
	}
}

func WithJSONError(err error) ResponseOption {
	return func(r *Response) {
		jsonWrapper(r, JSON{"error": err.Error()}, http.StatusInternalServerError)
	}
}

func jsonWrapper(r *Response, v JSON, status int) {
	r.Header().Set("Content-Type", "application/json")
	r.status = status

	// Encode to bytes instead of writing directly to avoid premature writing
	jsonBytes, err := json.Marshal(v)
	if err != nil {
		r.status = http.StatusInternalServerError
		r.body = []byte(err.Error())
	} else {
		r.body = jsonBytes
	}
}
