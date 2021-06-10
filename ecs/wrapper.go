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

type CallbackTask struct {
	Log        *logrus.Entry
	Token      string
	HBInterval string
	Sess       *session.Session
	sfnClient  *sfn.SFN
	hbTicker   *time.Ticker
	siTicker   *time.Ticker
	fn         Fn
	returnChan chan error
}

func (ct *CallbackTask) RegisterWorkerFunc(fn Fn) {
	ct.fn = fn
}

func (ct *CallbackTask) sendHeartbeat() {
	_, err := ct.sfnClient.SendTaskHeartbeat(&sfn.SendTaskHeartbeatInput{
		TaskToken: aws.String(ct.Token),
	})
	if err != nil {
		ct.returnChan <- err
	}
	ct.Log.Debugf("Successfully sent SendTaskHeartbeat back to sfn")
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
		ct.returnChan <- err
	}
	spotMsg, err := ct.getInstanceAction(token)
	if err != nil {
		ct.returnChan <- err
	}
	ct.Log.Debugf("Successfully Checked Spot Instance Interruption")

	emptyMsg := InterruptionMgs{}
	if spotMsg != emptyMsg {
		ct.Log.Warnf("Spot Interruption:  %+v", spotMsg)
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

func (ct *CallbackTask) init() {
	ct.sfnClient = sfn.New(ct.Sess)
	ct.returnChan = make(chan error, 10)
	interval, err := time.ParseDuration(ct.HBInterval)
	if err != nil {
		ct.sendFailure(err)
		ct.Log.Fatalf("Failed to Parse Heartbeat Duration. %v", err)
	}
	ct.hbTicker = time.NewTicker(interval)
	ct.siTicker = time.NewTicker(time.Minute)
}

func (ct *CallbackTask) Run() {
	ct.init()
	defer ct.hbTicker.Stop()
	go func() {
		err := ct.fn()
		ct.returnChan <- err
	}()

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
			ct.sendHeartbeat()
		case <-ct.siTicker.C:
			ct.checkSpotInterruption()
		}
	}
}
