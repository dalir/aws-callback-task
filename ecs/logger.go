package ecs

// Logger is a minimal logging interface for decoupling from any specific logging library.
type Logger interface {
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

// Slogger is a minimal interface matching the required slog.Logger methods.
type Slogger interface {
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

// SlogAdapter adapts a Slogger to the Logger interface.
type SlogAdapter struct {
	slogger Slogger
}

func NewSlogAdapter(slogger Slogger) *SlogAdapter {
	return &SlogAdapter{slogger: slogger}
}

func (a *SlogAdapter) Debug(msg string, args ...any) { a.slogger.Debug(msg, args...) }
func (a *SlogAdapter) Info(msg string, args ...any)  { a.slogger.Info(msg, args...) }
func (a *SlogAdapter) Warn(msg string, args ...any)  { a.slogger.Warn(msg, args...) }
func (a *SlogAdapter) Error(msg string, args ...any) { a.slogger.Error(msg, args...) }

// noopLogger is a Logger implementation that does nothing (for default fallback)
type noopLogger struct{}

func (n *noopLogger) Debug(msg string, args ...any) {}
func (n *noopLogger) Info(msg string, args ...any)  {}
func (n *noopLogger) Warn(msg string, args ...any)  {}
func (n *noopLogger) Error(msg string, args ...any) {}
