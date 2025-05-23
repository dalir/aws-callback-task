# aws-callback-task

A Go package for running callback tasks that interact with AWS Step Functions, with built-in support for heartbeats and spot instance interruption handling. Designed for use in ECS or EC2 environments where Step Functions' callback patterns are required.

## Features
- Sends heartbeats to AWS Step Functions to prevent task timeout
- Handles spot instance interruption notifications (EC2 Spot/Fargate)
- Retries on failure for heartbeats, success, and failure signals
- Simple function registration and execution pattern

## Installation

```
go get github.com/dalir/aws-callback-task
```

## Minimal Usage Example

```go
package main

import (
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

    // Create the callback task
    task := &ecs.CallbackTask{
        Log:        slog.Default(),
        Token:      "<STEP_FUNCTIONS_TASK_TOKEN>",
        HBInterval: "60s", // Heartbeat every 60 seconds
        AWSCfg:     cfg,
    }

    // Register the function to run
    task.RegisterWorkerFunc(func() (string, error) {
        // Your task logic here
        return `{"result": "success"}`, nil
    })

    // Run the task (blocks until completion)
    task.Run()
}
```

## Dependencies
- Go 1.22+
- AWS SDK v2 for Go

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.
