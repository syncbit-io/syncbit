package transfer

import (
	"context"
	"io"

	"golang.org/x/time/rate"
)

type RWCallback func(n int64)
type RWOption func(*ReaderWriter)

func RWWithReadLimiter(limiter *rate.Limiter) RWOption {
	return func(r *ReaderWriter) {
		r.readLimiter = limiter
	}
}

func RWWithWriteLimiter(limiter *rate.Limiter) RWOption {
	return func(r *ReaderWriter) {
		r.writeLimiter = limiter
	}
}

func RWWithIOReader(reader io.Reader) RWOption {
	return func(r *ReaderWriter) {
		r.reader = reader
	}
}

func RWWithIOWriter(writer io.Writer) RWOption {
	return func(r *ReaderWriter) {
		r.writer = writer
	}
}

func RWWithReaderCallback(callback RWCallback) RWOption {
	return func(r *ReaderWriter) {
		r.readerCallback = callback
	}
}

func RWWithWriterCallback(callback RWCallback) RWOption {
	return func(r *ReaderWriter) {
		r.writerCallback = callback
	}
}

func DefaultReaderWriter() *ReaderWriter {
	return &ReaderWriter{
		readLimiter:  DefaultRateLimiter(),
		writeLimiter: DefaultRateLimiter(),
	}
}

// ReaderWriter wraps an io.Reader and io.Writer and allows for context
// cancellation, rate limiting and callbacks.
//
// If callbacks are provided, they will be triggered after each read or write.
// NOTE: These are hot paths so don't block in the callback.
type ReaderWriter struct {
	reader         io.Reader
	writer         io.Writer
	readLimiter    *rate.Limiter
	writeLimiter   *rate.Limiter
	readerCallback RWCallback
	writerCallback RWCallback
}

// NewReaderWriter creates a new ReaderWriter.
func NewReaderWriter(opts ...RWOption) *ReaderWriter {
	r := DefaultReaderWriter()
	for _, opt := range opts {
		opt(r)
	}

	return r
}

// Transfer transfers data from the reader to the writer
func (r ReaderWriter) Transfer(ctx context.Context) (int64, error) {
	return io.Copy(r.Writer(ctx), r.Reader(ctx))
}

type ReaderWriterFunc func(p []byte) (int, error)

func (f ReaderWriterFunc) Read(p []byte) (int, error)  { return f(p) }
func (f ReaderWriterFunc) Write(p []byte) (int, error) { return f(p) }

// Reader creates a new io.Reader that wraps the underlying reader.
// Applies rate limiting and respects context cancellation.
// Triggers the callback after reading the data if provided.
//
// NOTE: Don't block in the callback.
func (r ReaderWriter) Reader(ctx context.Context) io.Reader {
	return ReaderWriterFunc(func(p []byte) (int, error) {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		default:
			if r.readLimiter != nil {
				if err := r.readLimiter.WaitN(ctx, len(p)); err != nil {
					return 0, err
				}
			}
			n, err := r.reader.Read(p)
			switch err {
			case nil:
				if r.readerCallback != nil {
					r.readerCallback(int64(n))
				}
				return n, nil
			default:
				return n, err
			}
		}
	})
}

// Writer creates a new io.Writer that wraps the underlying writer.
// Applies rate limiting and respects context cancellation.
// Triggers the callback after writing the data if provided.
//
// NOTE: Don't block in the callback.
func (w ReaderWriter) Writer(ctx context.Context) io.Writer {
	return ReaderWriterFunc(func(p []byte) (int, error) {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		default:
			if w.writeLimiter != nil {
				if err := w.writeLimiter.WaitN(ctx, len(p)); err != nil {
					return 0, err
				}
			}
			n, err := w.writer.Write(p)
			switch err {
			case nil:
				if w.writerCallback != nil {
					w.writerCallback(int64(n))
				}
				return n, nil
			default:
				return n, err
			}
		}
	})
}

func (r ReaderWriter) CloseReader() error {
	if closer, ok := r.reader.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

func (r ReaderWriter) CloseWriter() error {
	if closer, ok := r.writer.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}
