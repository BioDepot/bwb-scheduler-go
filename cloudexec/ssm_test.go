package cloudexec

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	st "github.com/aws/aws-sdk-go-v2/service/ssm/types"
	"github.com/aws/smithy-go"
	"testing"
)

type recordedSSM struct {
	fakeSSM
	command            *st.Command
	calls              int
	lose               bool
	output             string
	missingInvocations int
	polls              int
}

func (f *recordedSSM) SendCommand(_ context.Context, in *ssm.SendCommandInput, _ ...func(*ssm.Options)) (*ssm.SendCommandOutput, error) {
	f.calls++
	f.command = &st.Command{CommandId: aws.String("accepted-command"), Comment: in.Comment}
	if f.lose {
		f.lose = false
		return nil, fmt.Errorf("SSM accepted but response lost")
	}
	return &ssm.SendCommandOutput{Command: f.command}, nil
}
func (f *recordedSSM) ListCommands(context.Context, *ssm.ListCommandsInput, ...func(*ssm.Options)) (*ssm.ListCommandsOutput, error) {
	return &ssm.ListCommandsOutput{Commands: []st.Command{*f.command}}, nil
}
func (f *recordedSSM) GetCommandInvocation(context.Context, *ssm.GetCommandInvocationInput, ...func(*ssm.Options)) (*ssm.GetCommandInvocationOutput, error) {
	f.polls++
	if f.missingInvocations > 0 {
		f.missingInvocations--
		return nil, &smithy.GenericAPIError{Code: "InvocationDoesNotExist"}
	}
	return &ssm.GetCommandInvocationOutput{Status: st.CommandInvocationStatusSuccess, StandardOutputContent: aws.String(f.output)}, nil
}

func TestReconcileWaitsForSSMInvocationVisibility(t *testing.T) {
	c, ec2, r := fixture(t, "existing")
	ctx := context.Background()
	api := &recordedSSM{missingInvocations: 1, output: `{"state":"succeeded","process_stopped":true,"exit_code":0}`}
	c.Clients = func(context.Context, Profile) (EC2API, SSMAPI, STSAPI, error) { return ec2, api, fakeIdentity{}, nil }
	if _, e := c.Acquire(ctx, r); e != nil {
		t.Fatal(e)
	}
	command := RunnerRequest{OperationID: "science.start", Action: "start", CommandID: "science", Argv: []string{"/bin/true"}, WorkRoot: "/work/jobs/job", TimeoutSeconds: 60}
	if _, e := c.DispatchRunner(ctx, r.JobID, command); e != nil {
		t.Fatal(e)
	}
	if e := c.RecordOutcome(ctx, r.JobID, "succeeded", "succeeded", true); e != nil {
		t.Fatal(e)
	}
	state, e := c.Reconcile(ctx, r.JobID, true)
	if e != nil || state.CleanupOutcome != "complete" || api.polls != 2 || api.calls != 2 {
		t.Fatal(e, state.CleanupOutcome, api.polls, api.calls)
	}
}
func TestLostSSMReceiptAndDeliveryIsNotProcessSuccess(t *testing.T) {
	c, ec2, r := fixture(t, "existing")
	ctx := context.Background()
	api := &recordedSSM{lose: true, output: `{"state":"running","process_stopped":false}`}
	c.Clients = func(context.Context, Profile) (EC2API, SSMAPI, STSAPI, error) { return ec2, api, fakeIdentity{}, nil }
	if _, e := c.Acquire(ctx, r); e != nil {
		t.Fatal(e)
	}
	command := RunnerRequest{OperationID: "science.start", Action: "start", CommandID: "science", Argv: []string{"/bin/true"}, WorkRoot: "/work/jobs/job", TimeoutSeconds: 60}
	if _, e := c.DispatchRunner(ctx, r.JobID, command); e == nil {
		t.Fatal("lost response not injected")
	}
	recovered := *c
	id, e := recovered.DispatchRunner(ctx, r.JobID, command)
	if e != nil || id != "accepted-command" || api.calls != 1 {
		t.Fatal(id, e, api.calls)
	}
	result, e := recovered.PollRunner(ctx, r.JobID, command.OperationID)
	if e != nil || result.ProcessStopped || result.ProcessState != "running" {
		t.Fatal(result, e)
	}
	if _, e = c.Cleanup(ctx, r.JobID); e == nil {
		t.Fatal("SSM delivery success authorized cleanup of a running process")
	}
	api.output = `{"state":"succeeded","process_stopped":true,"exit_code":0}`
	result, e = recovered.PollRunner(ctx, r.JobID, command.OperationID)
	if e != nil || !result.ProcessStopped || result.ProcessState != "succeeded" {
		t.Fatal(result, e)
	}
	changed := command
	changed.Argv = []string{"/bin/false"}
	if _, e = c.DispatchRunner(ctx, r.JobID, changed); e == nil {
		t.Fatal("conflicting SSM operation accepted")
	}
}
