package ecs

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	stdoutlog "go.opentelemetry.io/otel/exporters/stdout/stdoutlog"
	log "go.opentelemetry.io/otel/log"
	sdklog "go.opentelemetry.io/otel/sdk/log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sfn"
)

// Fn defines a function type that returns a string and an error.
type Fn func(ctx context.Context) (string, error)

// Constants for retry attempts.
const HB_TICKER_RETRY = 3
const SEND_SUCCESS_RETRY = 3
const SEND_FAILURE_RETRY = 3

// CallbackOutput represents the output of a callback function,
// including any error and the JSON string result.
type CallbackOutput struct {
	Err        error
	JsonOutput string
}

// CallbackTask handles the execution of a task that communicates
// with AWS Step Functions and handles spot instance interruptions.
type CallbackTask struct {
	Logger             log.Logger // OpenTelemetry logger for logging events. If nil, a default logger is created in Run().
	Token              string     // Task token for communicating with AWS Step Functions.
	HBInterval         string     // Heartbeat interval duration string. A duration string is a possibly signed sequence of decimal numbers, each with optional fraction and a unit suffix, such as "300ms", "-1.5h" or "2h45m". Valid time units are "ns", "us" (or "µs"), "ms", "s", "m", "h".
	CheckSpotInterrupt bool       // Flag to check for spot instance interruptions.
	AWSCfg             aws.Config
	sfnClient           *sfn.Client        // AWS Step Functions client.
	hbTicker            *time.Ticker       // Ticker for sending heartbeats.
	siTicker            *time.Ticker       // Ticker for checking spot interruptions.
	fn                  Fn                 // Function to be executed by the task.
	returnChan          chan CallbackOutput // Channel for returning the result of the task.
	sigsChan            chan os.Signal      // Channel for capturing OS signals.
	hbRetryCounter      int
	successRetryCounter int
	failureRetryCounter int
}

// RegisterWorkerFunc registers the function to be executed by the task.
func (ct *CallbackTask) RegisterWorkerFunc(fn Fn) {
	ct.fn = fn
}

// sendHeartbeat sends a heartbeat signal to AWS Step Functions to prevent
// the task from timing out. Retries up to HB_TICKER_RETRY times if it fails.
func (ct *CallbackTask) sendHeartbeat(ctx context.Context) {
	_, err := ct.sfnClient.SendTaskHeartbeat(ctx, &sfn.SendTaskHeartbeatInput{
		TaskToken: aws.String(ct.Token),
	})
	if err != nil {
		ct.hbRetryCounter++
		rec := log.Record{}
		rec.SetTimestamp(time.Now())
		rec.SetSeverity(log.SeverityWarn)
		rec.SetBody(log.StringValue("SendTaskHeartbeat failed"))
		ct.Logger.Emit(ctx, rec)
		if ct.hbRetryCounter == HB_TICKER_RETRY {
			ct.returnChan <- CallbackOutput{
				Err: err,
			}
		}
	} else {
		rec := log.Record{}
		rec.SetTimestamp(time.Now())
		rec.SetSeverity(log.SeverityInfo)
		rec.SetBody(log.StringValue("Successfully sent SendTaskHeartbeat to Step Functions"))
		ct.Logger.Emit(ctx, rec)
	}
}

// InterruptionMgs represents a message indicating an interruption action,
// such as a spot instance termination.
type InterruptionMgs struct {
	Action string    `json:"action"` // The action taken, e.g., "terminate".
	Time   time.Time `json:"time"`   // The time the action was taken.
}

// getMetadataToken retrieves the metadata token required for subsequent
// metadata requests to the EC2 instance.
func (ct *CallbackTask) getMetadataToken(ctx context.Context) (token string, err error) {
	client := &http.Client{}
	req, err := http.NewRequestWithContext(ctx, "PUT", "http://169.254.169.254/latest/api/token", nil)
	if err != nil {
		return
	}
	req.Header.Add("X-aws-ec2-metadata-token-ttl-seconds", "30")
	resp, err := client.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return
	}
	token = string(body)
	return
}

// getInstanceAction retrieves the spot instance interruption action
// from the EC2 instance metadata service.
func (ct *CallbackTask) getInstanceAction(ctx context.Context, token string) (spotMsg InterruptionMgs, err error) {
	client := &http.Client{}
	req, err := http.NewRequestWithContext(ctx, "GET", "http://169.254.169.254/latest/meta-data/spot/instance-action", nil)
	if err != nil {
		return
	}
	req.Header.Add("X-aws-ec2-metadata-token", token)
	resp, err := client.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		err = fmt.Errorf("status not found")
		return
	}
	msg, err := io.ReadAll(resp.Body)
	if err != nil {
		return
	}
	if err = json.Unmarshal(msg, &spotMsg); err != nil {
		return
	}
	return
}

// checkSpotInterruption checks if the current EC2 instance is marked for
// interruption as part of a spot instance termination.
func (ct *CallbackTask) checkSpotInterruption(ctx context.Context) {
	token, err := ct.getMetadataToken(ctx)
	if err != nil {
		rec := log.Record{}
		rec.SetTimestamp(time.Now())
		rec.SetSeverity(log.SeverityWarn)
		rec.SetBody(log.StringValue("Failed to retrieve Metadata Token"))
		ct.Logger.Emit(ctx, rec)
	}
	spotMsg, err := ct.getInstanceAction(ctx, token)
	if err != nil {
		if err.Error() == "status not found" {
			rec := log.Record{}
			rec.SetTimestamp(time.Now())
			rec.SetSeverity(log.SeverityDebug)
			rec.SetBody(log.StringValue("No Spot Instance action is scheduled"))
			ct.Logger.Emit(ctx, rec)
		} else {
			rec := log.Record{}
			rec.SetTimestamp(time.Now())
			rec.SetSeverity(log.SeverityWarn)
			rec.SetBody(log.StringValue("Failed to retrieve Metadata Instance Action"))
			ct.Logger.Emit(ctx, rec)
		}
	} else {
		rec := log.Record{}
		rec.SetTimestamp(time.Now())
		rec.SetSeverity(log.SeverityInfo)
		rec.SetBody(log.StringValue("Successfully checked Spot Instance Interruption"))
		ct.Logger.Emit(ctx, rec)
	}

	emptyMsg := InterruptionMgs{}
	if spotMsg != emptyMsg {
		ct.spotInterrupted(spotMsg.Action)
	}
}

// spotInterrupted handles the event when a spot instance is interrupted.
// It logs the interruption and returns an error via the callback channel.
func (ct *CallbackTask) spotInterrupted(message string) {
	rec := log.Record{}
	rec.SetTimestamp(time.Now())
	rec.SetSeverity(log.SeverityWarn)
	rec.SetBody(log.StringValue("Spot Interruption Forced"))
	ct.Logger.Emit(context.Background(), rec)
	err := fmt.Errorf("InstanceInterruption")
	ct.returnChan <- CallbackOutput{
		Err: err,
	}
}

// sendSuccess sends a success signal to AWS Step Functions with the provided
// JSON string as the output. Retries up to SEND_SUCCESS_RETRY times if it fails.
func (ct *CallbackTask) sendSuccess(ctx context.Context, jsonString string) {
	if jsonString == "" {
		jsonString = `{"Report": "the task is completed successfully"}`
	}
	_, err := ct.sfnClient.SendTaskSuccess(ctx, &sfn.SendTaskSuccessInput{
		Output:    aws.String(jsonString),
		TaskToken: aws.String(ct.Token),
	})
	if err != nil {
		if ct.successRetryCounter == SEND_SUCCESS_RETRY {
			rec := log.Record{}
			rec.SetTimestamp(time.Now())
			rec.SetSeverity(log.SeverityError)
			rec.SetBody(log.StringValue("Failed in sendSuccess"))
			rec.AddAttributes(log.String("error", err.Error()))
			ct.Logger.Emit(ctx, rec)
		} else {
			ct.successRetryCounter++
			rec := log.Record{}
			rec.SetTimestamp(time.Now())
			rec.SetSeverity(log.SeverityWarn)
			rec.SetBody(log.StringValue("Failed in sendSuccess (retry)"))
			ct.Logger.Emit(ctx, rec)
			time.Sleep(5 * time.Second)
			ct.sendSuccess(ctx, jsonString)
		}
	} else {
		rec := log.Record{}
		rec.SetTimestamp(time.Now())
		rec.SetSeverity(log.SeverityInfo)
		rec.SetBody(log.StringValue("Successfully sent SendTaskSuccess to Step Functions"))
		ct.Logger.Emit(ctx, rec)
	}
}

// sendFailure sends a failure signal to AWS Step Functions with the provided
// error message. Retries up to SEND_FAILURE_RETRY times if it fails.
func (ct *CallbackTask) sendFailure(ctx context.Context, errMsg error) {
	_, err := ct.sfnClient.SendTaskFailure(ctx, &sfn.SendTaskFailureInput{
		Error:     aws.String(errMsg.Error()),
		TaskToken: aws.String(ct.Token),
	})
	if err != nil {
		if ct.failureRetryCounter == SEND_FAILURE_RETRY {
			rec := log.Record{}
			rec.SetTimestamp(time.Now())
			rec.SetSeverity(log.SeverityError)
			rec.SetBody(log.StringValue("Failed in sendFailure"))
			rec.AddAttributes(log.String("error", err.Error()))
			ct.Logger.Emit(ctx, rec)
		} else {
			ct.failureRetryCounter++
			time.Sleep(5 * time.Second)
			ct.sendFailure(ctx, errMsg)
		}
	} else {
		rec := log.Record{}
		rec.SetTimestamp(time.Now())
		rec.SetSeverity(log.SeverityError)
		rec.SetBody(log.StringValue("Successfully sent SendTaskFailure to Step Functions"))
		ct.Logger.Emit(ctx, rec)
	}
}

// Run starts the execution of the CallbackTask, including sending heartbeats,
// checking for spot interruptions, and handling the task execution result.
func (ct *CallbackTask) Run(ctx context.Context) {
	ct.sfnClient = sfn.NewFromConfig(ct.AWSCfg)
	if ct.Logger == nil {
		exp, err := stdoutlog.New()
		if err != nil {
			panic("failed to create stdoutlog exporter: " + err.Error())
		}
		provider := sdklog.NewLoggerProvider(sdklog.WithProcessor(sdklog.NewBatchProcessor(exp)))
		ct.Logger = provider.Logger("CallbackTask")
	}
	ct.returnChan = make(chan CallbackOutput, 10)
	ct.sigsChan = make(chan os.Signal, 1)
	signal.Notify(ct.sigsChan, syscall.SIGTERM)

	interval, err := time.ParseDuration(ct.HBInterval)
	if err != nil {
		ct.sendFailure(ctx, err)
		rec := log.Record{}
		rec.SetTimestamp(time.Now())
		rec.SetSeverity(log.SeverityError)
		rec.SetBody(log.StringValue("Failed to parse HBInterval"))
		rec.AddAttributes(log.String("error", err.Error()))
		ct.Logger.Emit(ctx, rec)
		return
	}
	ct.hbTicker = time.NewTicker(interval)
	ct.siTicker = time.NewTicker(110 * time.Second)
	defer ct.hbTicker.Stop()
	defer ct.siTicker.Stop()

	go func() {
		output, err := ct.fn(ctx)
		ct.returnChan <- CallbackOutput{
			Err:        err,
			JsonOutput: output,
		}
	}()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for {
			select {
			case callbackOutput := <-ct.returnChan:
				if callbackOutput.Err != nil {
					ct.sendFailure(ctx, callbackOutput.Err)
					rec := log.Record{}
					rec.SetTimestamp(time.Now())
					rec.SetSeverity(log.SeverityError)
					rec.SetBody(log.StringValue("Worker function error"))
					rec.AddAttributes(log.String("error", callbackOutput.Err.Error()))
					ct.Logger.Emit(ctx, rec)
					wg.Done()
					return
				}
				ct.sendSuccess(ctx, callbackOutput.JsonOutput)
				wg.Done()
				return
			case <-ct.hbTicker.C:
				go ct.sendHeartbeat(ctx)
			case <-ct.siTicker.C:
				if ct.CheckSpotInterrupt && (os.Getenv("AWS_EXECUTION_ENV") == "AWS_ECS_EC2") {
					go ct.checkSpotInterruption(ctx)
				}
			case sig := <-ct.sigsChan:
				if ct.CheckSpotInterrupt && (os.Getenv("AWS_EXECUTION_ENV") == "AWS_ECS_FARGATE") {
					go ct.spotInterrupted(sig.String())
				}
			}
		}
	}()
	wg.Wait()
}
