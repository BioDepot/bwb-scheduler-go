package cloudexec

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/aws/smithy-go"
	"go.temporal.io/sdk/temporal"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	st "github.com/aws/aws-sdk-go-v2/service/ssm/types"
	"go-scheduler/durable"
)

type Dispatch struct {
	Request     *RunnerRequest `json:"request,omitempty"`
	Action      string         `json:"action"`
	CommandID   string         `json:"command_id"`
	Fingerprint string         `json:"fingerprint"`
	RequestedAt time.Time      `json:"requested_at"`
}
type RunnerRequest struct {
	Stream           string   `json:"stream,omitempty"`
	Cursor           int64    `json:"cursor,omitempty"`
	OperationID      string   `json:"operation_id"`
	Action           string   `json:"action"`
	CommandID        string   `json:"command_id"`
	WorkRoot         string   `json:"work_root"`
	Argv             []string `json:"argv,omitempty"`
	TimeoutSeconds   int      `json:"timeout_seconds,omitempty"`
	MinimumFreeBytes int64    `json:"minimum_free_bytes,omitempty"`
}
type RunnerResult struct {
	DeliveryState  string          `json:"delivery_state"`
	ProcessState   string          `json:"process_state"`
	ProcessStopped bool            `json:"process_stopped"`
	ExitCode       *int            `json:"exit_code,omitempty"`
	Data           json.RawMessage `json:"data,omitempty"`
}

func quote(s string) string { return "'" + strings.ReplaceAll(s, "'", "'\"'\"'") + "'" }
func commandLine(p Profile, action string, payload []byte) string {
	code := "import hashlib,os; p='/opt/workbench/runner.py'; assert 'sha256:'+hashlib.sha256(open(p,'rb').read()).hexdigest()==" + fmt.Sprintf("%q", p.Runtime.RunnerHash) + "; os.execvp('python3',['python3',p," + fmt.Sprintf("%q", action) + "," + fmt.Sprintf("%q", base64.StdEncoding.EncodeToString(payload)) + "])"
	return "python3 -c " + quote(code)
}
func (c *Controller) DispatchRunner(ctx context.Context, id string, r RunnerRequest) (string, error) {
	if !identifier.MatchString(r.OperationID) || !identifier.MatchString(r.CommandID) {
		return "", fmt.Errorf("invalid runner operation identity")
	}
	switch r.Action {
	case "prepare", "runtime", "start", "status", "cancel", "unmount", "logs":
	default:
		return "", fmt.Errorf("unsupported runner action")
	}
	var commandID string
	err := c.Store.With(ctx, stateKey(id), func(entry *durable.Entry) error {
		var state State
		if err := entry.Read(&state); err != nil {
			return err
		}
		if state.InstanceID == "" {
			return fmt.Errorf("instance unavailable")
		}
		_, api, _, err := c.Clients(ctx, state.Profile)
		if err != nil {
			return err
		}
		ready, err := ssmReady(ctx, api, state.InstanceID)
		if err != nil {
			return err
		}
		if !ready {
			return temporal.NewApplicationError("SSM agent is not ready", "NotReady")
		}
		payload := map[string]any{"job_id": id, "command_id": r.CommandID, "work_root": r.WorkRoot, "argv": r.Argv, "timeout_seconds": r.TimeoutSeconds, "runtime": state.Profile.Runtime, "volumes": state.Volumes, "minimum_free_bytes": r.MinimumFreeBytes, "stream": r.Stream, "cursor": r.Cursor}
		raw, err := json.Marshal(payload)
		if err != nil {
			return err
		}
		if len(raw) > 16000 {
			return fmt.Errorf("runner request exceeds SSM inline limit")
		}
		fingerprint := token(r.Action, string(raw))
		if state.Dispatches == nil {
			state.Dispatches = map[string]Dispatch{}
		}
		dispatch, exists := state.Dispatches[r.OperationID]
		if exists && dispatch.Fingerprint != fingerprint {
			return fmt.Errorf("SSM operation identity conflicts")
		}
		if dispatch.CommandID != "" {
			commandID = dispatch.CommandID
			return nil
		}
		comment := "wb-" + token(id, r.OperationID, fingerprint)
		if exists {
			var next *string
			for {
				out, err := api.ListCommands(ctx, &ssm.ListCommandsInput{InstanceId: aws.String(state.InstanceID), NextToken: next, Filters: []st.CommandFilter{{Key: st.CommandFilterKeyInvokedAfter, Value: aws.String(dispatch.RequestedAt.Add(-time.Minute).UTC().Format(time.RFC3339))}}})
				if err != nil {
					return err
				}
				for _, command := range out.Commands {
					if aws.ToString(command.Comment) == comment {
						dispatch.CommandID = aws.ToString(command.CommandId)
						break
					}
				}
				if dispatch.CommandID != "" || out.NextToken == nil {
					break
				}
				next = out.NextToken
			}
			if dispatch.CommandID == "" && time.Since(dispatch.RequestedAt) < 30*time.Second {
				return temporal.NewApplicationError("SSM dispatch receipt not yet visible; retry reconciliation", "NotReady")
			}
		} else {
			dispatch = Dispatch{Action: r.Action, Fingerprint: fingerprint, RequestedAt: time.Now().UTC()}
			if r.Action == "start" || r.Action == "prepare" {
				copy := r
				dispatch.Request = &copy
			}
			if r.Action == "start" {
				state.ProcessStopped = false
			}
			if r.Action == "prepare" {
				state.MountsReleased = false
			}
			state.Dispatches[r.OperationID] = dispatch
			if err = entry.Write(state); err != nil {
				return err
			}
		}
		if dispatch.CommandID == "" {
			timeout := int32(3600)
			if r.Action == "start" || r.Action == "status" || r.Action == "cancel" {
				timeout = 120
			}
			out, err := api.SendCommand(ctx, &ssm.SendCommandInput{InstanceIds: []string{state.InstanceID}, DocumentName: aws.String("AWS-RunShellScript"), Comment: aws.String(comment), TimeoutSeconds: aws.Int32(timeout), Parameters: map[string][]string{"commands": {commandLine(state.Profile, r.Action, raw)}, "executionTimeout": {fmt.Sprint(timeout)}}})
			if err != nil {
				return err
			}
			if out.Command == nil || out.Command.CommandId == nil {
				return fmt.Errorf("missing SSM command receipt")
			}
			dispatch.CommandID = aws.ToString(out.Command.CommandId)
		}
		state.Dispatches[r.OperationID] = dispatch
		commandID = dispatch.CommandID
		return entry.Write(state)
	})
	return commandID, err
}
func (c *Controller) PollRunner(ctx context.Context, id, operationID string) (RunnerResult, error) {
	state, err := c.Status(ctx, id)
	if err != nil {
		return RunnerResult{}, err
	}
	dispatch, ok := state.Dispatches[operationID]
	if !ok || dispatch.CommandID == "" {
		return RunnerResult{DeliveryState: "unknown"}, fmt.Errorf("SSM dispatch has no recovered receipt")
	}
	_, api, _, err := c.Clients(ctx, state.Profile)
	if err != nil {
		return RunnerResult{}, err
	}
	out, err := api.GetCommandInvocation(ctx, &ssm.GetCommandInvocationInput{CommandId: aws.String(dispatch.CommandID), InstanceId: aws.String(state.InstanceID)})
	if err != nil {
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) && apiErr.ErrorCode() == "InvocationDoesNotExist" {
			return RunnerResult{}, temporal.NewApplicationError("SSM receipt not yet visible", "NotReady")
		}
		return RunnerResult{}, err
	}
	result := RunnerResult{DeliveryState: string(out.Status)}
	if out.Status != st.CommandInvocationStatusSuccess {
		return result, nil
	}
	var data struct {
		State          string `json:"state"`
		ProcessStopped bool   `json:"process_stopped"`
		ExitCode       *int   `json:"exit_code"`
	}
	raw := []byte(aws.ToString(out.StandardOutputContent))
	if err = json.Unmarshal(raw, &data); err != nil {
		return result, fmt.Errorf("runner did not return structured evidence")
	}
	result.ProcessState, result.ProcessStopped, result.ExitCode, result.Data = data.State, data.ProcessStopped, data.ExitCode, raw
	if (dispatch.Action == "start" || dispatch.Action == "status" || dispatch.Action == "cancel") && data.ProcessStopped || dispatch.Action == "unmount" && data.State == "unmounted" {
		err = c.Store.With(ctx, stateKey(id), func(e *durable.Entry) error {
			var current State
			if err := e.Read(&current); err != nil {
				return err
			}
			if data.ProcessStopped {
				current.ProcessStopped = true
			}
			if dispatch.Action == "unmount" && data.State == "unmounted" {
				current.MountsReleased = true
			}
			return e.Write(current)
		})
		if err != nil {
			return result, err
		}
	}
	return result, nil
}
func (c *Controller) RecordOutcome(ctx context.Context, id, compute, publication string, stopped bool) error {
	return c.Store.With(ctx, stateKey(id), func(e *durable.Entry) error {
		var s State
		if err := e.Read(&s); err != nil {
			return err
		}
		s.ComputeOutcome = compute
		s.PublicationOutcome = publication
		s.ProcessStopped = stopped
		s.ActiveCollection = ""
		return e.Write(s)
	})
}

func (c *Controller) SaveStageResult(ctx context.Context, id string, result StageResult) error {
	return c.Store.Write(ctx, "aws-result/"+id, result)
}
func (c *Controller) ReadStageResult(ctx context.Context, id string) (StageResult, error) {
	var result StageResult
	err := c.Store.Read(ctx, "aws-result/"+id, &result)
	return result, err
}

func (c *Controller) BeginCollection(ctx context.Context, id, collection string) (State, error) {
	var state State
	e := c.Store.With(ctx, stateKey(id), func(entry *durable.Entry) error {
		if e := entry.Read(&state); e != nil {
			return e
		}
		if state.ActiveCollection == collection {
			return nil
		}
		if state.ActiveCollection != "" || state.ComputeOutcome != "succeeded" || state.CleanupOutcome != "retained" || !state.ProcessStopped {
			return fmt.Errorf("resources are not available for publication recovery")
		}
		until, e := time.Parse(time.RFC3339, state.RetainUntil)
		if e != nil || !time.Now().Before(until) {
			return fmt.Errorf("retention expired")
		}
		state.ActiveCollection = collection
		// Prevent independent cleanup between remount and the first transfer start.
		state.MountsReleased = false
		return entry.Write(state)
	})
	return state, e
}
