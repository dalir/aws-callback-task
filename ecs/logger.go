package ecs

import (
	"fmt"
	"log"

	"github.com/rs/zerolog"
	"github.com/sirupsen/logrus"
	"go.uber.org/zap"
)

// Package ecs provides a minimal Logger interface for decoupling from any specific logging library.
//
// # Creating a Logger Adapter
//
// To use your own logger with CallbackTask, implement the Logger interface:
//
//   type Logger interface {
//       Debug(msg string, args ...any)
//       Info(msg string, args ...any)
//       Warn(msg string, args ...any)
//       Error(msg string, args ...any)
//   }
//
// If your logger does not match this interface, create an adapter:
//
//   type MyLoggerAdapter struct {
//       Logger *mylogger.Logger
//   }
//   func (l *MyLoggerAdapter) Debug(msg string, args ...any) { l.Logger.Debug(msg, args...) }
//   func (l *MyLoggerAdapter) Info(msg string, args ...any)  { l.Logger.Info(msg, args...) }
//   func (l *MyLoggerAdapter) Warn(msg string, args ...any)  { l.Logger.Warn(msg, args...) }
//   func (l *MyLoggerAdapter) Error(msg string, args ...any) { l.Logger.Error(msg, args...) }
//
// Then pass your adapter to CallbackTask:
//
//   task := &ecs.CallbackTask{
//       Log: &MyLoggerAdapter{Logger: myLogger},
//       ...
//   }
//
// See ready-to-use adapters for zap, logrus, and zerolog below.

// Logger is a minimal logging interface for decoupling from any specific logging library.
// Any logger that implements these methods can be used with this package.
type Logger interface {
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

// LogAdapter adapts a Logger (such as slog.Logger) to the Logger interface used by this package.
// This allows you to use Go's slog or any compatible logger with aws-callback-task.
type LogAdapter struct {
	logger Logger
}

// NewLogAdapter wraps a Logger (such as slog.Logger) to conform to the Logger interface.
func NewLogAdapter(logger Logger) *LogAdapter {
	return &LogAdapter{logger: logger}
}

// Debug logs a debug-level message using the underlying logger.
func (a *LogAdapter) Debug(msg string, args ...any) { a.logger.Debug(msg, args...) }

// Info logs an info-level message using the underlying logger.
func (a *LogAdapter) Info(msg string, args ...any) { a.logger.Info(msg, args...) }

// Warn logs a warning-level message using the underlying logger.
func (a *LogAdapter) Warn(msg string, args ...any) { a.logger.Warn(msg, args...) }

// Error logs an error-level message using the underlying logger.
func (a *LogAdapter) Error(msg string, args ...any) { a.logger.Error(msg, args...) }

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

// --- Adapters for popular loggers ---

// ZapLoggerAdapter adapts a *zap.Logger to the Logger interface.
// Requires: import "go.uber.org/zap"
// Note: The 'args' parameter is ignored. If you want to log structured fields, use zap.Fields directly in your own adapter.
type ZapLoggerAdapter struct {
	Logger *zap.Logger
}

// NewZapLoggerAdapter returns a Logger interface for a zap.Logger.
func NewZapLoggerAdapter(logger *zap.Logger) *ZapLoggerAdapter {
	return &ZapLoggerAdapter{Logger: logger}
}

func (z *ZapLoggerAdapter) Debug(msg string, args ...any) { z.Logger.Debug(msg) }
func (z *ZapLoggerAdapter) Info(msg string, args ...any)  { z.Logger.Info(msg) }
func (z *ZapLoggerAdapter) Warn(msg string, args ...any)  { z.Logger.Warn(msg) }
func (z *ZapLoggerAdapter) Error(msg string, args ...any) { z.Logger.Error(msg) }

// LogrusLoggerAdapter adapts a *logrus.Logger to the Logger interface.
// Requires: import "github.com/sirupsen/logrus"
type LogrusLoggerAdapter struct {
	Logger *logrus.Logger
}

// NewLogrusLoggerAdapter returns a Logger interface for a logrus.Logger.
func NewLogrusLoggerAdapter(logger *logrus.Logger) *LogrusLoggerAdapter {
	return &LogrusLoggerAdapter{Logger: logger}
}

func (l *LogrusLoggerAdapter) Debug(msg string, args ...any) {
	l.Logger.Debug(append([]interface{}{msg}, args...)...)
}
func (l *LogrusLoggerAdapter) Info(msg string, args ...any) {
	l.Logger.Info(append([]interface{}{msg}, args...)...)
}
func (l *LogrusLoggerAdapter) Warn(msg string, args ...any) {
	l.Logger.Warn(append([]interface{}{msg}, args...)...)
}
func (l *LogrusLoggerAdapter) Error(msg string, args ...any) {
	l.Logger.Error(append([]interface{}{msg}, args...)...)
}

// ZerologLoggerAdapter adapts a zerolog.Logger to the Logger interface.
// Requires: import "github.com/rs/zerolog"
// Note: The 'args' parameter is ignored. If you want to log structured fields, use zerolog.Event methods directly in your own adapter.
type ZerologLoggerAdapter struct {
	Logger zerolog.Logger
}

// NewZerologLoggerAdapter returns a Logger interface for a zerolog.Logger.
func NewZerologLoggerAdapter(logger zerolog.Logger) *ZerologLoggerAdapter {
	return &ZerologLoggerAdapter{Logger: logger}
}

func (z *ZerologLoggerAdapter) Debug(msg string, args ...any) { z.Logger.Debug().Msg(msg) }
func (z *ZerologLoggerAdapter) Info(msg string, args ...any)  { z.Logger.Info().Msg(msg) }
func (z *ZerologLoggerAdapter) Warn(msg string, args ...any)  { z.Logger.Warn().Msg(msg) }
func (z *ZerologLoggerAdapter) Error(msg string, args ...any) { z.Logger.Error().Msg(msg) }

// StdLoggerAdapter adapts a standard library log.Logger to the Logger interface.
// Requires: import "log"
type StdLoggerAdapter struct {
	Logger *log.Logger
}

// NewStdLoggerAdapter returns a Logger interface for a standard log.Logger.
func NewStdLoggerAdapter(logger *log.Logger) *StdLoggerAdapter {
	return &StdLoggerAdapter{Logger: logger}
}

func (l *StdLoggerAdapter) Debug(msg string, args ...any) {
	l.Logger.Printf("[DEBUG] %s", fmt.Sprintf(msg, args...))
}
func (l *StdLoggerAdapter) Info(msg string, args ...any) {
	l.Logger.Printf("[INFO] %s", fmt.Sprintf(msg, args...))
}
func (l *StdLoggerAdapter) Warn(msg string, args ...any) {
	l.Logger.Printf("[WARN] %s", fmt.Sprintf(msg, args...))
}
func (l *StdLoggerAdapter) Error(msg string, args ...any) {
	l.Logger.Printf("[ERROR] %s", fmt.Sprintf(msg, args...))
}
