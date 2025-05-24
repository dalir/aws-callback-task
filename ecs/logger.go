package ecs

// Logger is a minimal logging interface for decoupling from any specific logging library.
type Logger interface {
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

// SlogAdapter adapts slog.Logger to the Logger interface.
type SlogAdapter struct {
	slogger any // use any to avoid direct slog import here
}

func NewSlogAdapter(slogger any) *SlogAdapter {
	return &SlogAdapter{slogger: slogger}
}

func (a *SlogAdapter) Debug(msg string, args ...any) {
	// type assertion to *slog.Logger
	if l, ok := a.slogger.(interface{ Debug(string, ...any) }); ok {
		l.Debug(msg, args...)
	}
}
func (a *SlogAdapter) Info(msg string, args ...any) {
	if l, ok := a.slogger.(interface{ Info(string, ...any) }); ok {
		l.Info(msg, args...)
	}
}
func (a *SlogAdapter) Warn(msg string, args ...any) {
	if l, ok := a.slogger.(interface{ Warn(string, ...any) }); ok {
		l.Warn(msg, args...)
	}
}
func (a *SlogAdapter) Error(msg string, args ...any) {
	if l, ok := a.slogger.(interface{ Error(string, ...any) }); ok {
		l.Error(msg, args...)
	}
}

// noopLogger is a Logger implementation that does nothing (for default fallback)
type noopLogger struct{}

func (n *noopLogger) Debug(msg string, args ...any) {}
func (n *noopLogger) Info(msg string, args ...any)  {}
func (n *noopLogger) Warn(msg string, args ...any)  {}
func (n *noopLogger) Error(msg string, args ...any) {}
