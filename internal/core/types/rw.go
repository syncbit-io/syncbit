package types

import (
	"context"
	"io"
)

type RWCallback func(n int64)
type RWOption func(*ReaderWriter)

func RWWithReadLimiter(limiter *RateLimiter) RWOption {
	return func(r *ReaderWriter) {
		r.readLimiter = limiter
	}
}

func RWWithWriteLimiter(limiter *RateLimiter) RWOption {
	return func(r *ReaderWriter) {
		r.writeLimiter = limiter
	}
}

func RWWithIOReader(reader io.Reader) RWOption {
	return func(r *ReaderWriter) {
		r.reader = reader
	}
}

func RWWithIOWriter(writer io.WriterAt) RWOption {
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

type ReaderFunc func(p []byte) (int, error)

func (f ReaderFunc) Read(p []byte) (int, error) { return f(p) }

type ReaderFromFunc func(r io.Reader) (int64, error)

func (f ReaderFromFunc) ReadFrom(r io.Reader) (int64, error) { return f(r) }

type WriterFunc func(p []byte) (int, error)

func (f WriterFunc) Write(p []byte) (int, error) { return f(p) }

type WriterAtFunc func(p []byte, off int64) (int, error)

func (f WriterAtFunc) WriteAt(p []byte, off int64) (int, error) { return f(p, off) }

// ReaderWriter wraps an io.Reader and io.Writer and allows for context
// cancellation, rate limiting and callbacks.
//
// If callbacks are provided, they will be triggered after each read or write.
// NOTE: These are hot paths so don't block in the callback.
type ReaderWriter struct {
	reader         io.Reader
	writer         io.WriterAt
	readLimiter    *RateLimiter
	writeLimiter   *RateLimiter
	readerCallback RWCallback
	writerCallback RWCallback
}

func DefaultReaderWriter() *ReaderWriter {
	return &ReaderWriter{
		readLimiter:  DefaultRateLimiter(),
		writeLimiter: DefaultRateLimiter(),
	}
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
func (rw ReaderWriter) Transfer(ctx context.Context) (int64, error) {
	return io.Copy(rw.Writer(ctx), rw.Reader(ctx))
}

// Reader creates a new io.Reader that wraps the underlying reader.
func (rw ReaderWriter) Reader(ctx context.Context) io.Reader {
	return ReaderFunc(func(p []byte) (int, error) {
		return rw.read(ctx, p)
	})
}

// Writer creates a new io.Writer that wraps the underlying writer.
func (rw ReaderWriter) Writer(ctx context.Context) io.Writer {
	return WriterFunc(func(p []byte) (int, error) {
		return rw.write(ctx, p, 0)
	})
}

// WriterAt creates a new io.WriterAt that wraps the underlying writer.
func (rw ReaderWriter) WriterAt(ctx context.Context) WriterAtFunc {
	return WriterAtFunc(func(p []byte, off int64) (int, error) {
		return rw.write(ctx, p, off)
	})
}

// CloseReader closes the underlying reader.
func (rw ReaderWriter) CloseReader() error {
	if closer, ok := rw.reader.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

// CloseWriter closes the underlying writer.
func (rw ReaderWriter) CloseWriter() error {
	if closer, ok := rw.writer.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

// ReaderFrom creates a new io.ReaderFrom that wraps the underlying reader.
func (rw ReaderWriter) ReaderFrom(ctx context.Context) ReaderFromFunc {
	return ReaderFromFunc(func(r io.Reader) (int64, error) {
		return io.Copy(rw.Writer(ctx), r)
	})
}

// read reads data from the underlying reader.
// Applies rate limiting and respects context cancellation.
// Triggers the callback after reading the data if provided.
//
// NOTE: Don't block in the callback.
func (rw ReaderWriter) read(ctx context.Context, p []byte) (int, error) {
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
		if rw.readLimiter != nil {
			if err := rw.readLimiter.WaitN(ctx, len(p)); err != nil {
				return 0, err
			}
		}
		n, err := rw.reader.Read(p)
		if err == nil && rw.readerCallback != nil {
			rw.readerCallback(int64(n))
		}
		return n, err
	}
}

// Writer creates a new io.Writer that wraps the underlying writer.
// Applies rate limiting and respects context cancellation.
// Triggers the callback after writing the data if provided.
//
// NOTE: Don't block in the callback.
func (rw ReaderWriter) write(ctx context.Context, p []byte, off int64) (int, error) {
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
		if rw.writeLimiter != nil {
			if err := rw.writeLimiter.WaitN(ctx, len(p)); err != nil {
				return 0, err
			}
		}
		n, err := rw.writer.WriteAt(p, off)
		if err == nil && rw.writerCallback != nil {
			rw.writerCallback(int64(n))
		}
		return n, err
	}
}

type WriterReadFromProvider struct {
	reader io.ReaderFrom
	writer io.Writer
	closer func()
}

func (wrf WriterReadFromProvider) GetReadFrom(w io.Writer) (WriterReadFromProvider, func()) {
	return wrf, wrf.closer
}

func (wrf WriterReadFromProvider) ReadFrom(r io.Reader) (int64, error) {
	return wrf.reader.ReadFrom(r)
}

func (wrf WriterReadFromProvider) Write(p []byte) (int, error) {
	return wrf.writer.Write(p)
}

func (rw *ReaderWriter) WriterReadFromAdapter(ctx context.Context) WriterReadFromProvider {
	return WriterReadFromProvider{
		reader: rw.ReaderFrom(ctx),
		writer: rw.Writer(ctx),
		closer: func() { rw.CloseReader() },
	}
}
