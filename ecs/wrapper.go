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
	"time"
)

type Fn func() error

const HB_TICKER_RETRY = 3

var hbRetryCounter = 0

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
	returnChan         chan error
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
			ct.returnChan <- err
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
		ct.Log.Warnf("Spot Interruption Forced:  %+v", spotMsg)
		err = fmt.Errorf("InstanceInterruption")
		ct.returnChan <- err
	}
}

func (ct *CallbackTask) sendSuccess() {
	_, err := ct.sfnClient.SendTaskSuccess(&sfn.SendTaskSuccessInput{
		Output:    aws.String(`{"Report": "the task is completed successfully"}`),
		TaskToken: aws.String(ct.Token),
	})
	if err != nil {
		ct.Log.Fatalf("Failed in sendSuccess. %v", err)
	}
	ct.Log.Info("Successfully sent SendTaskSuccess back to sfn")

}

func (ct *CallbackTask) sendFailure(errMsg error) {
	_, err := ct.sfnClient.SendTaskFailure(&sfn.SendTaskFailureInput{
		Error:     aws.String(errMsg.Error()),
		TaskToken: aws.String(ct.Token),
	})
	if err != nil {
		ct.Log.Fatalf("Failed in sendFailure. %v", err)
	}
	ct.Log.Errorf("Successfully sent SendTaskFailure back to sfn. Error message %v", err)
}

func (ct *CallbackTask) Run() {
	ct.sfnClient = sfn.New(ct.Sess)
	ct.returnChan = make(chan error, 10)
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
		err := ct.fn()
		ct.returnChan <- err
	}()

	go func() {
		for {
			select {
			case err := <-ct.returnChan:
				if err != nil {
					ct.sendFailure(err)
					ct.Log.Fatalf("%v", err)
				}
				ct.sendSuccess()
				return
			case <-ct.hbTicker.C:
				go ct.sendHeartbeat()
			case <-ct.siTicker.C:
				if ct.CheckSpotInterrupt {
					go ct.checkSpotInterruption()
				}
			}
		}
	}()
}
