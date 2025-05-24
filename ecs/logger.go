package ecs

// Logger is a minimal logging interface for decoupling from any specific logging library.
type Logger interface {
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

// SlogAdapter adapts a Logger to the Logger interface.
type SlogAdapter struct {
	logger Logger
}

func NewSlogAdapter(logger Logger) *SlogAdapter {
	return &SlogAdapter{logger: logger}
}

func (a *SlogAdapter) Debug(msg string, args ...any) { a.logger.Debug(msg, args...) }
func (a *SlogAdapter) Info(msg string, args ...any)  { a.logger.Info(msg, args...) }
func (a *SlogAdapter) Warn(msg string, args ...any)  { a.logger.Warn(msg, args...) }
func (a *SlogAdapter) Error(msg string, args ...any) { a.logger.Error(msg, args...) }

// noopLogger is a Logger implementation that does nothing (for default fallback)
type noopLogger struct{}

func (n *noopLogger) Debug(msg string, args ...any) {}
func (n *noopLogger) Info(msg string, args ...any)  {}
func (n *noopLogger) Warn(msg string, args ...any)  {}
func (n *noopLogger) Error(msg string, args ...any) {}
