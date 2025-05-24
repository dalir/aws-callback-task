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
Debug(msg string, args ...any)
Info(msg string, args ...any)
Warn(msg string, args ...any)
Error(msg string, args ...any)
```

A convenience adapter for Go's slog is provided, but you must pass a logger that implements the required methods (Debug, Info, Warn, Error):

```go
// Wrap your slog.Logger with ecs.NewSlogAdapter
// Your slog.Logger must implement Debug, Info, Warn, and Error methods.
logger := ecs.NewSlogAdapter(slog.Default())
```

If you do not provide a logger, a no-op logger will be used and no logs will be emitted.

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

    // Create the callback task with slog logger
    logger := ecs.NewSlogAdapter(slog.Default())
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
