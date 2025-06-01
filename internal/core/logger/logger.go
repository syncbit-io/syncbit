package logger

import (
	"log/slog"
	"os"
	"time"

	"github.com/lmittmann/tint"
	"github.com/mattn/go-isatty"
)

type Level slog.Level

var (
	LevelDebug = Level(slog.LevelDebug)
	LevelInfo  = Level(slog.LevelInfo)
	LevelWarn  = Level(slog.LevelWarn)
	LevelError = Level(slog.LevelError)
)

var defaultLevel = Level(slog.LevelInfo)

func SetDefaultLevel(level Level) {
	defaultLevel = level
}

type HandlerOption func(*tint.Options)

func WithTimeFormat(format string) HandlerOption {
	return func(opts *tint.Options) {
		opts.TimeFormat = format
	}
}

func WithNoColor(noColor bool) HandlerOption {
	return func(opts *tint.Options) {
		opts.NoColor = noColor
	}
}

func DefaultHandlerOptions() []HandlerOption {
	return []HandlerOption{
		WithTimeFormat(time.Stamp),
		WithNoColor(false),
	}
}

func NewHandlerOptions(opts ...HandlerOption) *tint.Options {
	timeFormat := time.Stamp
	isTerminal := isatty.IsTerminal(os.Stderr.Fd())
	if !isTerminal {
		timeFormat = time.RFC3339
	}
	tintOpts := &tint.Options{
		Level:      slog.Level(defaultLevel),
		NoColor:    !isTerminal,
		TimeFormat: timeFormat,
	}
	for _, opt := range opts {
		opt(tintOpts)
	}
	return tintOpts
}

func DefaultHandler(opts ...HandlerOption) slog.Handler {
	return tint.NewHandler(os.Stderr, NewHandlerOptions(opts...))
}

type LoggerOption func(*Logger)

func WithName(name string) LoggerOption {
	return func(l *Logger) {
		l.name = name
	}
}

func WithLevel(level Level) LoggerOption {
	return func(l *Logger) {
		l.level = level
	}
}

func WithHandler(handler slog.Handler) LoggerOption {
	return func(l *Logger) {
		l.handler = handler
	}
}

func WithHandlerOptions(opts ...HandlerOption) LoggerOption {
	return func(l *Logger) {
		l.opts = opts
	}
}

// Logger is a wrapper around the global logger instance
type Logger struct {
	*slog.Logger
	level   Level
	handler slog.Handler
	name    string
	opts    []HandlerOption
}

// NewLogger creates a new logger instance
func NewLogger(opts ...LoggerOption) *Logger {
	l := &Logger{
		name:  "root",
		level: defaultLevel,
		opts:  DefaultHandlerOptions(),
	}
	for _, opt := range opts {
		opt(l)
	}
	if l.handler == nil {
		// Create handler with the correct level
		handlerOpts := append(l.opts, func(tintOpts *tint.Options) {
			tintOpts.Level = slog.Level(l.level)
		})
		l.handler = DefaultHandler(handlerOpts...)
	}
	l.Logger = slog.New(l.handler).WithGroup(l.name)
	return l
}

// Group creates a new group attribute
func (l *Logger) Group(key string, args ...any) slog.Attr {
	return slog.Group(key, args...)
}

// With creates a new logger with the given attributes
func (l *Logger) With(args ...any) *slog.Logger {
	return l.Logger.With(args...)
}

func (l *Logger) WithGroup(group string) *Logger {
	return &Logger{
		Logger:  l.Logger.WithGroup(group),
		level:   l.level,
		handler: l.handler,
	}
}

func (l *Logger) Fatal(msg string, args ...any) {
	l.Logger.Error(msg, args...)
	os.Exit(1)
}
