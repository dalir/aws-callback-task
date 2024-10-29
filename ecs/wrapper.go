package ecs

import (
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sfn"
	"github.com/sirupsen/logrus"
	"io"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// Fn defines a function type that returns a string and an error.
type Fn func() (string, error)

// Constants for retry attempts.
const HB_TICKER_RETRY = 3
const SEND_SUCCESS_RETRY = 3
const SEND_FAILURE_RETRY = 3

// Global counters for retry attempts.
var hbRetryCounter = 0
var successRetryCounter = 0
var failureRetryCounter = 0

// CallbackOutput represents the output of a callback function,
// including any error and the JSON string result.
type CallbackOutput struct {
	Err        error
	JsonOutput string
}

// CallbackTask handles the execution of a task that communicates
// with AWS Step Functions and handles spot instance interruptions.
type CallbackTask struct {
	Log                *logrus.Entry       // Logger for logging events.
	Token              string              // Task token for communicating with AWS Step Functions.
	HBInterval         string              // Heartbeat interval duration string. A duration string is a possibly signed sequence of decimal numbers, each with optional fraction and a unit suffix, such as "300ms", "-1.5h" or "2h45m". Valid time units are "ns", "us" (or "µs"), "ms", "s", "m", "h".
	CheckSpotInterrupt bool                // Flag to check for spot instance interruptions.
	Sess               *session.Session    // AWS session for making API requests.
	sfnClient          *sfn.SFN            // AWS Step Functions client.
	hbTicker           *time.Ticker        // Ticker for sending heartbeats.
	siTicker           *time.Ticker        // Ticker for checking spot interruptions.
	fn                 Fn                  // Function to be executed by the task.
	returnChan         chan CallbackOutput // Channel for returning the result of the task.
	sigsChan           chan os.Signal      // Channel for capturing OS signals.
}

// RegisterWorkerFunc registers the function to be executed by the task.
func (ct *CallbackTask) RegisterWorkerFunc(fn Fn) {
	ct.fn = fn
}

// sendHeartbeat sends a heartbeat signal to AWS Step Functions to prevent
// the task from timing out. Retries up to HB_TICKER_RETRY times if it fails.
func (ct *CallbackTask) sendHeartbeat() {
	_, err := ct.sfnClient.SendTaskHeartbeat(&sfn.SendTaskHeartbeatInput{
		TaskToken: aws.String(ct.Token),
	})
	if err != nil {
		hbRetryCounter++
		ct.Log.Warnf("SendTaskHeartbeat failed. Retry number: %d, Error: %v", hbRetryCounter, err)
		if hbRetryCounter == HB_TICKER_RETRY {
			ct.returnChan <- CallbackOutput{
				Err: err,
			}
		}
	} else {
		ct.Log.Debugf("Successfully sent SendTaskHeartbeat to Step Functions")
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
func (ct *CallbackTask) getMetadataToken() (token string, err error) {
	client := &http.Client{}
	req, err := http.NewRequest("PUT", "http://169.254.169.254/latest/api/token", nil)
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
	token = string(body)
	return
}

// getInstanceAction retrieves the spot instance interruption action
// from the EC2 instance metadata service.
func (ct *CallbackTask) getInstanceAction(token string) (spotMsg InterruptionMgs, err error) {
	client := &http.Client{}
	req, err := http.NewRequest("GET", "http://169.254.169.254/latest/meta-data/spot/instance-action", nil)
	if err != nil {
		return
	}
	req.Header.Add("X-aws-ec2-metadata-token", token)
	resp, err := client.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	msg, err := io.ReadAll(resp.Body)
	if err != nil {
		return
	}
	json.Unmarshal(msg, &spotMsg)
	return
}

// checkSpotInterruption checks if the current EC2 instance is marked for
// interruption as part of a spot instance termination.
func (ct *CallbackTask) checkSpotInterruption() {
	token, err := ct.getMetadataToken()
	if err != nil {
		ct.Log.Warnf("Failed to retrieve Metadata Token. %v", err)
	}
	spotMsg, err := ct.getInstanceAction(token)
	if err != nil {
		ct.Log.Warnf("Failed to retrieve Metadata Instance Action. %v", err)
	} else {
		ct.Log.Debugf("Successfully checked Spot Instance Interruption")
	}

	emptyMsg := InterruptionMgs{}
	if spotMsg != emptyMsg {
		ct.spotInterrupted(spotMsg.Action)
	}
}

// spotInterrupted handles the event when a spot instance is interrupted.
// It logs the interruption and returns an error via the callback channel.
func (ct *CallbackTask) spotInterrupted(message string) {
	ct.Log.Warnf("Spot Interruption Forced: %s", message)
	err := fmt.Errorf("InstanceInterruption")
	ct.returnChan <- CallbackOutput{
		Err: err,
	}
}

// sendSuccess sends a success signal to AWS Step Functions with the provided
// JSON string as the output. Retries up to SEND_SUCCESS_RETRY times if it fails.
func (ct *CallbackTask) sendSuccess(jsonString string) {
	if jsonString == "" {
		jsonString = `{"Report": "the task is completed successfully"}`
	}
	_, err := ct.sfnClient.SendTaskSuccess(&sfn.SendTaskSuccessInput{
		Output:    aws.String(jsonString),
		TaskToken: aws.String(ct.Token),
	})
	if err != nil {
		if successRetryCounter == SEND_SUCCESS_RETRY {
			ct.Log.Fatalf("Failed in sendSuccess. %v", err)
		} else {
			successRetryCounter++
			ct.Log.Warnf("Failed in sendSuccess. %v, retry counter: %d", err, successRetryCounter)
			time.Sleep(5 * time.Second)
			ct.sendSuccess(jsonString)
		}
	} else {
		ct.Log.Info("Successfully sent SendTaskSuccess to Step Functions")
	}
}

// sendFailure sends a failure signal to AWS Step Functions with the provided
// error message. Retries up to SEND_FAILURE_RETRY times if it fails.
func (ct *CallbackTask) sendFailure(errMsg error) {
	_, err := ct.sfnClient.SendTaskFailure(&sfn.SendTaskFailureInput{
		Error:     aws.String(errMsg.Error()),
		TaskToken: aws.String(ct.Token),
	})
	if err != nil {
		if failureRetryCounter == SEND_FAILURE_RETRY {
			ct.Log.Fatalf("Failed in sendFailure. %v", err)
		} else {
			failureRetryCounter++
			ct.Log.Warnf("Failed in sendFailure. %v, retry counter: %d", err, failureRetryCounter)
			time.Sleep(5 * time.Second)
			ct.sendFailure(errMsg)
		}
	} else {
		ct.Log.Errorf("Successfully sent SendTaskFailure to Step Functions. Error message: %s", errMsg.Error())
	}
}

// Run starts the execution of the CallbackTask, including sending heartbeats,
// checking for spot interruptions, and handling the task execution result.
func (ct *CallbackTask) Run() {
	ct.sfnClient = sfn.New(ct.Sess)
	ct.returnChan = make(chan CallbackOutput, 10)
	ct.sigsChan = make(chan os.Signal, 1)
	signal.Notify(ct.sigsChan, syscall.SIGTERM)

	interval, err := time.ParseDuration(ct.HBInterval)
	if err != nil {
		ct.sendFailure(err)
		ct.Log.Fatalf("Failed to Parse Heartbeat Duration. %v", err)
	}
	ct.hbTicker = time.NewTicker(interval)
	ct.siTicker = time.NewTicker(110 * time.Second)
	defer ct.hbTicker.Stop()
	defer ct.siTicker.Stop()

	go func() {
		output, err := ct.fn()
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
					ct.sendFailure(callbackOutput.Err)
					ct.Log.Fatalf("%v", callbackOutput.Err)
					wg.Done()
				}
				ct.sendSuccess(callbackOutput.JsonOutput)
				wg.Done()
			case <-ct.hbTicker.C:
				go ct.sendHeartbeat()
			case <-ct.siTicker.C:
				if ct.CheckSpotInterrupt && (os.Getenv("AWS_EXECUTION_ENV") == "AWS_ECS_EC2") {
					go ct.checkSpotInterruption()
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
