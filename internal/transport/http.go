package transport

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"

	"golang.org/x/net/http2"
)

func DefaultHTTPClient() *http.Client {
	return &http.Client{
		Transport: &http2.Transport{
			AllowHTTP: true,
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
	}
}

type HTTPTransferOption func(*HTTPTransfer)

func HTTPWithClient(c *http.Client) HTTPTransferOption {
	return func(t *HTTPTransfer) {
		t.client = c
	}
}

type HTTPTransfer struct {
	client *http.Client
}

func DefaultHTTPTransfer() *HTTPTransfer {
	return &HTTPTransfer{
		client: DefaultHTTPClient(),
	}
}

func NewHTTPTransfer(opts ...HTTPTransferOption) *HTTPTransfer {
	ht := DefaultHTTPTransfer()

	for _, opt := range opts {
		opt(ht)
	}

	return ht
}

type HTTPRequestOption func(*http.Request)

func HTTPRequestHeaders(h map[string]string) HTTPRequestOption {
	return func(req *http.Request) {
		for k, v := range h {
			req.Header.Set(k, v)
		}
	}
}

func HTTPRequestRange(start, end int64) HTTPRequestOption {
	return func(req *http.Request) {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))
	}
}

func HTTPRequestBody(body []byte) HTTPRequestOption {
	return func(req *http.Request) {
		req.Body = io.NopCloser(bytes.NewReader(body))
	}
}

type HTTPResponseCallback func(*http.Response) error

func (ht *HTTPTransfer) Do(
	ctx context.Context,
	method, url string,
	respCb HTTPResponseCallback,
	reqOpts ...HTTPRequestOption,
) error {
	req, err := http.NewRequestWithContext(ctx, method, url, nil)
	if err != nil {
		return err
	}

	for _, opt := range reqOpts {
		opt(req)
	}

	resp, err := ht.client.Do(req)
	if err != nil {
		return err
	}

	return respCb(resp)
}

func (ht *HTTPTransfer) Get(ctx context.Context, url string, respCb HTTPResponseCallback, reqOpts ...HTTPRequestOption) error {
	return ht.Do(ctx, http.MethodGet, url, respCb, reqOpts...)
}

func (ht *HTTPTransfer) Post(ctx context.Context, url string, respCb HTTPResponseCallback, reqOpts ...HTTPRequestOption) error {
	return ht.Do(ctx, http.MethodPost, url, respCb, reqOpts...)
}

func (ht *HTTPTransfer) Put(ctx context.Context, url string, respCb HTTPResponseCallback, reqOpts ...HTTPRequestOption) error {
	return ht.Do(ctx, http.MethodPut, url, respCb, reqOpts...)
}

func (ht *HTTPTransfer) Delete(ctx context.Context, url string, respCb HTTPResponseCallback, reqOpts ...HTTPRequestOption) error {
	return ht.Do(ctx, http.MethodDelete, url, respCb, reqOpts...)
}

func (ht *HTTPTransfer) Head(ctx context.Context, url string, respCb HTTPResponseCallback, reqOpts ...HTTPRequestOption) error {
	return ht.Do(ctx, http.MethodHead, url, respCb, reqOpts...)
}
