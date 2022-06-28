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

type Fn func() (string, error)

const HB_TICKER_RETRY = 3
const SEND_SUCCESS_RETRY = 3
const SEND_FAILURE_RETRY = 3

var hbRetryCounter = 0
var successRetryCounter = 0
var failureRetryCounter = 0

type CallbackOutput struct {
	Err        error
	JsonOutput string
}

type CallbackTask struct {
	Log                *logrus.Entry
	Token              string
	HBInterval         string
	CheckSpotInterrupt bool
	Sess               *session.Session
	sfnClient          *sfn.SFN
	hbTicker           *time.Ticker
	siTicker           *time.Ticker
	fn                 Fn
	returnChan         chan CallbackOutput
	sigsChan           chan os.Signal
}

func (ct *CallbackTask) RegisterWorkerFunc(fn Fn) {
	ct.fn = fn
}

func (ct *CallbackTask) sendHeartbeat() {
	_, err := ct.sfnClient.SendTaskHeartbeat(&sfn.SendTaskHeartbeatInput{
		TaskToken: aws.String(ct.Token),
	})
	if err != nil {
		hbRetryCounter++
		ct.Log.Warnf("sent SendTaskHeartbeat failed. Retry number: %d, Error: %v", hbRetryCounter, err)
		if hbRetryCounter == HB_TICKER_RETRY {
			ct.returnChan <- CallbackOutput{
				Err: err,
			}
		}
	} else {
		ct.Log.Debugf("Successfully sent SendTaskHeartbeat back to sfn")
	}
}

type InterruptionMgs struct {
	Action string    `json:"action"`
	Time   time.Time `json:"time"`
}

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

func (ct *CallbackTask) checkSpotInterruption() {
	token, err := ct.getMetadataToken()
	if err != nil {
		ct.Log.Warnf("Failed to retrieve Metadata Token. %v", err)
	}
	spotMsg, err := ct.getInstanceAction(token)
	if err != nil {
		ct.Log.Warnf("Failed to retrieve Metadata Insgance Action. %v", err)
	} else {
		ct.Log.Debugf("Successfully Checked Spot Instance Interruption")
	}

	emptyMsg := InterruptionMgs{}
	if spotMsg != emptyMsg {
		ct.spotInterrupted(spotMsg.Action)
	}
}

func (ct *CallbackTask) spotInterrupted(message string) {
	ct.Log.Warnf("Spot Interruption Forced: %s", message)
	err := fmt.Errorf("InstanceInterruption")
	ct.returnChan <- CallbackOutput{
		Err: err,
	}
}

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
		ct.Log.Info("Successfully sent SendTaskSuccess back to sfn")
	}

}

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
		ct.Log.Errorf("Successfully sent SendTaskFailure back to sfn. Error message %v", err)
	}
}

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
					ct.sendFailure(err)
					ct.Log.Fatalf("%v", err)
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
