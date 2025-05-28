# aws-callback-task

A Go package for running callback tasks that interact with AWS Step Functions, with built-in support for heartbeats and spot instance interruption handling. Designed for use in ECS or EC2 environments where Step Functions' callback patterns are required.

## Features
- **Heartbeat Support:** Automatically sends heartbeats to AWS Step Functions to prevent task timeout.
- **Spot Instance Interruption Handling:** Detects and gracefully handles EC2 Spot and Fargate interruption notifications.
- **Retry Logic:** Retries heartbeats, success, and failure signals with exponential backoff.
- **Context Propagation:** All operations are context-aware, supporting cancellation and timeouts.
- **Pluggable Logging:** Use any logger that implements a simple interface, or use the built-in no-op logger.
- **Simple Worker Registration:** Register your own function to be executed as the callback task.
- **Minimal Dependencies:** Only depends on AWS SDK v2 and standard Go libraries.

## Installation

```
go get github.com/dalir/aws-callback-task
```

## Logger Integration

This package is decoupled from any specific logging library. You can use any logger that implements the following interface:

```go
// ecs.Logger interface
type Logger interface {
    Debug(msg string, args ...any)
    Info(msg string, args ...any)
    Warn(msg string, args ...any)
    Error(msg string, args ...any)
}
```

If you do not provide a logger, a no-op logger will be used and no logs will be emitted.

### Ready-to-Use Logger Adapters

Adapters are provided for popular logging libraries:

- **zap** (go.uber.org/zap):
  ```go
  import "go.uber.org/zap"
  import "github.com/dalir/aws-callback-task/ecs"
  
  zapLogger := zap.NewExample()
  logger := ecs.NewZapLoggerAdapter(zapLogger)
  task := &ecs.CallbackTask{Log: logger, ...}
  ```
- **logrus** (github.com/sirupsen/logrus):
  ```go
  import "github.com/sirupsen/logrus"
  import "github.com/dalir/aws-callback-task/ecs"
  
  logrusLogger := logrus.New()
  logger := ecs.NewLogrusLoggerAdapter(logrusLogger)
  task := &ecs.CallbackTask{Log: logger, ...}
  ```
- **zerolog** (github.com/rs/zerolog):
  ```go
  import "github.com/rs/zerolog"
  import "github.com/dalir/aws-callback-task/ecs"
  
  zerologLogger := zerolog.New(os.Stdout)
  logger := ecs.NewZerologLoggerAdapter(zerologLogger)
  task := &ecs.CallbackTask{Log: logger, ...}
  ```
- **slog** (log/slog, Go 1.21+):
  ```go
  import "log/slog"
  import "github.com/dalir/aws-callback-task/ecs"
  
  logger := slog.Default()
  task := &ecs.CallbackTask{Log: logger, ...}
  ```
- **standard log.Logger** (log):
  ```go
  import "log"
  import "github.com/dalir/aws-callback-task/ecs"
  
  stdLogger := log.New(os.Stdout, "", log.LstdFlags)
  logger := ecs.NewStdLoggerAdapter(stdLogger)
  task := &ecs.CallbackTask{Log: logger, ...}
  ```

## How to Create a Custom Logger Adapter

If you use a different logging library, you can create your own adapter by implementing the `Logger` interface:

```go
type MyLoggerAdapter struct {
    Logger *mylogger.Logger
}

func (l *MyLoggerAdapter) Debug(msg string, args ...any) { l.Logger.Debug(msg, args...) }
func (l *MyLoggerAdapter) Info(msg string, args ...any)  { l.Logger.Info(msg, args...) }
func (l *MyLoggerAdapter) Warn(msg string, args ...any)  { l.Logger.Warn(msg, args...) }
func (l *MyLoggerAdapter) Error(msg string, args ...any) { l.Logger.Error(msg, args...) }
```

Then use your adapter:

```go
task := &ecs.CallbackTask{
    Log: &MyLoggerAdapter{Logger: myLogger},
    // ...
}
```

## Minimal Usage Example

```go
package main

import (
    "context"
    "github.com/dalir/aws-callback-task/ecs"
    "github.com/aws/aws-sdk-go-v2/config"
    "log/slog"
)

func main() {
    // Load AWS config
    cfg, err := config.LoadDefaultConfig(context.TODO())
    if err != nil {
        panic(err)
    }

    // Use slog directly (Go 1.21+)
    logger := slog.Default()
    task := &ecs.CallbackTask{
        Log:        logger, // Any logger implementing ecs.Logger
        Token:      "<STEP_FUNCTIONS_TASK_TOKEN>",
        HBInterval: "60s", // Heartbeat every 60 seconds
        AWSCfg:     cfg,
    }

    // Register the function to run
    task.RegisterWorkerFunc(func(ctx context.Context) (string, error) {
        // Your task logic here, now context-aware
        return `{"result": "success"}`, nil
    })

    // Run the task (blocks until completion)
    task.Run(context.TODO())
}
```

## Dependencies
- Go 1.22+
- AWS SDK v2 for Go

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.

## Import Example

```go
import "github.com/dalir/aws-callback-task/ecs"
// ... uses ecs.Logger, etc.
```
