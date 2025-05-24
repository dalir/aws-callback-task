package ecs

// Logger is a minimal logging interface for decoupling from any specific logging library.
// Any logger that implements these methods can be used with this package.
type Logger interface {
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

// SlogAdapter adapts a Logger (such as slog.Logger) to the Logger interface used by this package.
// This allows you to use Go's slog or any compatible logger with aws-callback-task.
type SlogAdapter struct {
	logger Logger
}

// NewSlogAdapter wraps a Logger (such as slog.Logger) to conform to the Logger interface.
func NewSlogAdapter(logger Logger) *SlogAdapter {
	return &SlogAdapter{logger: logger}
}

// Debug logs a debug-level message using the underlying logger.
func (a *SlogAdapter) Debug(msg string, args ...any) { a.logger.Debug(msg, args...) }

// Info logs an info-level message using the underlying logger.
func (a *SlogAdapter) Info(msg string, args ...any) { a.logger.Info(msg, args...) }

// Warn logs a warning-level message using the underlying logger.
func (a *SlogAdapter) Warn(msg string, args ...any) { a.logger.Warn(msg, args...) }

// Error logs an error-level message using the underlying logger.
func (a *SlogAdapter) Error(msg string, args ...any) { a.logger.Error(msg, args...) }

// noopLogger is a Logger implementation that does nothing.
// It is used as a default when no logger is provided, so no logs are emitted.
type noopLogger struct{}

// Debug is a no-op for noopLogger.
func (n *noopLogger) Debug(msg string, args ...any) {}

// Info is a no-op for noopLogger.
func (n *noopLogger) Info(msg string, args ...any) {}

// Warn is a no-op for noopLogger.
func (n *noopLogger) Warn(msg string, args ...any) {}

// Error is a no-op for noopLogger.
func (n *noopLogger) Error(msg string, args ...any) {}
