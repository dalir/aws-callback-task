package ecs

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sfn"
	"github.com/sirupsen/logrus"
	"time"
)

type Fn func() error

type CallbackTask struct {
	Log        *logrus.Entry
	Token      string
	HBInterval string
	Sess       *session.Session
	sfnClient  *sfn.SFN
	ticker     *time.Ticker
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
	ct.Log.Info("Successfully sent SendTaskFailure back to sfn")
}

func (ct *CallbackTask) init() {
	ct.sfnClient = sfn.New(ct.Sess)
	ct.returnChan = make(chan error, 10)
	interval, err := time.ParseDuration(ct.HBInterval)
	if err != nil {
		ct.sendFailure(err)
		ct.Log.Fatalf("Failed to Parse Heartbeat Duration. %v", err)
	}
	ct.ticker = time.NewTicker(interval)
}

func (ct *CallbackTask) Run() {
	ct.init()
	defer ct.ticker.Stop()
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
		case <-ct.ticker.C:
			ct.sendHeartbeat()
		}
	}
}
