package handlers

import (
	"context"
	"net/http"
	"net/url"
	"os"
	"path/filepath"

	"syncbit/internal/runner"
	"syncbit/internal/transfer"

	"golang.org/x/time/rate"
)

type HTTPDownloadHandlerOption func(*HTTPDownloadHandler)

func HTTPDownloadHandlerWithLimiter(limiter *rate.Limiter) HTTPDownloadHandlerOption {
	return func(t *HTTPDownloadHandler) {
		t.limiter = limiter
	}
}

// HTTPDownloadHandler is a handler that downloads a file from a URL.
type HTTPDownloadHandler struct {
	localBasePath string // local base path
	localJobPath  string // local job path -> localBasePath/localJobPath
	baseUrl       string // base url of the remote file path
	remotePath    string // remote file path -> baseUrl/remotePath -> localBasePath/localJobPath/remotePath
	limiter       *rate.Limiter
}

// NewHTTPDownloadHandler creates a new HTTPDownloadHandler for downloading files from a URL.
func NewHTTPDownloadHandler(
	localBasePath, localJobPath,
	baseUrl, remotePath string,
	opts ...HTTPDownloadHandlerOption,
) *HTTPDownloadHandler {

	t := &HTTPDownloadHandler{
		localBasePath: localBasePath,
		localJobPath:  localJobPath,
		baseUrl:       baseUrl,
		remotePath:    remotePath,
		limiter:       transfer.DefaultRateLimiter(),
	}

	for _, opt := range opts {
		opt(t)
	}

	return t
}

func (t *HTTPDownloadHandler) Run(ctx context.Context, self *runner.Job) error {
	u, err := url.JoinPath(t.baseUrl, t.remotePath)
	if err != nil {
		return err
	}

	filePath := filepath.Dir(t.remotePath)
	fileName := filepath.Base(t.remotePath)
	destPath := filepath.Join(t.localBasePath, t.localJobPath, filePath)

	if err := os.MkdirAll(destPath, 0755); err != nil {
		return err
	}

	destFile := filepath.Join(destPath, fileName)
	fileHandle, err := os.Create(destFile)
	if err != nil {
		return err
	}
	defer fileHandle.Close()

	ht := transfer.NewHTTPTransfer(
		transfer.HTTPWithClient(transfer.DefaultHTTPClient()),
	)

	htCallback := func(resp *http.Response) error {
		defer resp.Body.Close()
		self.Tracker().SetTotal(resp.ContentLength)
		self.Tracker().SetCurrent(0)
		rw := transfer.NewReaderWriter(
			transfer.RWWithIOWriter(fileHandle),
			transfer.RWWithIOReader(resp.Body),
			transfer.RWWithReadLimiter(t.limiter),
			transfer.RWWithReaderCallback(func(n int64) {
				self.Tracker().IncCurrent(n)
			}),
		)
		_, err := rw.Transfer(ctx)
		return err
	}

	return ht.Get(ctx, u, htCallback)
}
