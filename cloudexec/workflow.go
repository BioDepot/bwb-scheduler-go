package cloudexec

import (
	"errors"
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

type StageRequest struct {
	CollectionID      string        `json:"collection_id,omitempty"`
	Identity          Request       `json:"identity"`
	Queue             string        `json:"queue"`
	Command           RunnerRequest `json:"command"`
	BootstrapSeconds  int           `json:"bootstrap_seconds"`
	JobSeconds        int           `json:"job_seconds"`
	CollectionSeconds int           `json:"collection_seconds"`
	// Commands are fixed, reviewed transfer/publication invocations using the
	// same pinned runner. They run only after provisioning/before cleanup.
	Inputs  []RunnerRequest `json:"inputs"`
	Outputs []RunnerRequest `json:"outputs"`
}
type StageResult struct {
	State        State          `json:"state"`
	Compute      string         `json:"compute"`
	Publication  string         `json:"publication"`
	Cleanup      string         `json:"cleanup"`
	Runner       RunnerResult   `json:"runner"`
	Publications []RunnerResult `json:"publications"`
	Error        string         `json:"error,omitempty"`
}
type StageStatus struct {
	Phase       string `json:"phase"`
	JobID       string `json:"job_id"`
	Compute     string `json:"compute"`
	Publication string `json:"publication"`
	Cleanup     string `json:"cleanup"`
}

func activityCtx(ctx workflow.Context, queue string) workflow.Context {
	return workflow.WithActivityOptions(ctx, workflow.ActivityOptions{TaskQueue: queue, StartToCloseTimeout: 2 * time.Minute, ScheduleToCloseTimeout: 5 * time.Minute, WaitForCancellation: true, RetryPolicy: &temporal.RetryPolicy{InitialInterval: 2 * time.Second, MaximumInterval: 20 * time.Second, MaximumAttempts: 3, NonRetryableErrorTypes: []string{"Configuration", "Conflict"}}})
}
func RunnerOperation(ctx workflow.Context, queue, id string, r RunnerRequest, deadline time.Time) (RunnerResult, error) {
	act := activityCtx(ctx, queue)
	var commandID string
	for {
		err := workflow.ExecuteActivity(act, "cloud.DispatchRunner", id, r).Get(ctx, &commandID)
		if err == nil {
			break
		}
		var app *temporal.ApplicationError
		if !errors.As(err, &app) || app.Type() != "NotReady" || !workflow.Now(ctx).Before(deadline) {
			return RunnerResult{}, err
		}
		if err = workflow.Sleep(ctx, 5*time.Second); err != nil {
			return RunnerResult{}, err
		}
	}
	for {
		if !workflow.Now(ctx).Before(deadline) {
			return RunnerResult{}, fmt.Errorf("runner operation deadline exceeded")
		}
		var result RunnerResult
		if err := workflow.ExecuteActivity(act, "cloud.PollRunner", id, r.OperationID).Get(ctx, &result); err != nil {
			var app *temporal.ApplicationError
			if !errors.As(err, &app) || app.Type() != "NotReady" {
				return result, err
			}
		}
		switch result.DeliveryState {
		case "Success":
			return result, nil
		case "Failed", "Cancelled", "TimedOut", "Cancelling":
			return result, fmt.Errorf("SSM delivery %s", result.DeliveryState)
		}
		if err := workflow.Sleep(ctx, 3*time.Second); err != nil {
			return result, err
		}
	}
}
func waitCommand(ctx workflow.Context, req StageRequest, command RunnerRequest, deadline time.Time) (RunnerResult, error) {
	// Bound history growth for long computations. Cancellation remains immediate
	// through Temporal cancellation, independent of this status polling interval.
	interval := deadline.Sub(workflow.Now(ctx)) / 500
	if interval < 3*time.Second {
		interval = 3 * time.Second
	}
	result, err := RunnerOperation(ctx, req.Queue, req.Identity.JobID, command, deadline)
	if err != nil {
		return result, err
	}
	for poll := 0; !result.ProcessStopped; poll++ {
		if !workflow.Now(ctx).Before(deadline) {
			return result, fmt.Errorf("scientific command deadline exceeded")
		}
		if err = workflow.Sleep(ctx, interval); err != nil {
			return result, err
		}
		status := command
		status.Action = "status"
		status.OperationID = fmt.Sprintf("%s.poll.%d", command.OperationID, poll)
		result, err = RunnerOperation(ctx, req.Queue, req.Identity.JobID, status, deadline)
		if err != nil {
			return result, err
		}
		if result.ProcessState == "unknown" {
			return result, fmt.Errorf("scientific process outcome is unknown")
		}
	}
	if result.ProcessState != "succeeded" {
		return result, fmt.Errorf("scientific command %s", result.ProcessState)
	}
	return result, nil
}

// Transfers may restart their idempotent data client after a confirmed process
// failure. Scientific commands never use this retry path.
func waitTransfer(ctx workflow.Context, req StageRequest, command RunnerRequest, deadline time.Time, active *RunnerRequest) (RunnerResult, error) {
	var result RunnerResult
	var err error
	for attempt := 0; attempt < 3; attempt++ {
		current := command
		if attempt > 0 {
			current.CommandID += fmt.Sprintf(".try%d", attempt)
			current.OperationID += fmt.Sprintf(".try%d", attempt)
		}
		*active = current
		result, err = waitCommand(ctx, req, current, deadline)
		if err == nil || !result.ProcessStopped || result.ProcessState != "failed" || !workflow.Now(ctx).Before(deadline) {
			return result, err
		}
	}
	return result, err
}

// StageWorkflow is new; no legacy histories are changed. All external I/O is
// delegated to activities, and all waits/deadlines use Temporal's workflow clock.
func StageWorkflow(ctx workflow.Context, req StageRequest) (result StageResult, returned error) {
	if req.Queue == "" || req.BootstrapSeconds < 1 || req.JobSeconds < 1 || req.CollectionSeconds < 1 {
		return result, temporal.NewNonRetryableApplicationError("invalid stage bounds", "Configuration", nil)
	}
	status := StageStatus{Phase: "acquiring_compute", JobID: req.Identity.JobID, Compute: "not_started", Publication: "not_started", Cleanup: "pending"}
	if err := workflow.SetQueryHandler(ctx, "status", func() (StageStatus, error) { return status, nil }); err != nil {
		return result, err
	}
	act := activityCtx(ctx, req.Queue)
	acquired := false
	preparedMounts := false
	operationPrefix := ""
	if req.CollectionID != "" {
		operationPrefix = req.CollectionID + "."
	}
	started := false
	activeCommand := req.Command
	stopped := true
	result.Compute, result.Publication = "not_started", "not_started"
	// Persist terminal results independently of Temporal retention and errors.
	defer func() {
		detached, _ := workflow.NewDisconnectedContext(ctx)
		if returned != nil {
			result.Error = returned.Error() + "; " + result.Error
		}
		if e := workflow.ExecuteActivity(activityCtx(detached, req.Queue), "cloud.SaveStageResult", req.Identity.JobID, result).Get(detached, nil); e != nil {
			result.Error += "; terminal evidence persistence failed: " + e.Error()
			if returned == nil {
				returned = e
			}
		}
	}()
	// Cleanup has its own bounded disconnected context. A server hard timeout
	// cannot execute this defer; the independent durable reconciliation path is
	// required for terminated workflows and controller outages.
	defer func() {
		if returned != nil {
			result.Error = returned.Error()
		}
		if !acquired {
			return
		}
		cleanup, _ := workflow.NewDisconnectedContext(ctx)
		cleanup = activityCtx(cleanup, req.Queue)
		limit := workflow.Now(cleanup).Add(8 * time.Minute)
		status.Phase = "cleaning_up"
		if started && !stopped {
			cancel := activeCommand
			cancel.Action = "cancel"
			cancel.OperationID = operationPrefix + "cleanup.cancel"
			receipt, err := RunnerOperation(cleanup, req.Queue, req.Identity.JobID, cancel, limit)
			for poll := 0; err == nil && !receipt.ProcessStopped && workflow.Now(cleanup).Before(limit); poll++ {
				if err = workflow.Sleep(cleanup, 2*time.Second); err != nil {
					break
				}
				cancel.Action = "status"
				cancel.OperationID = fmt.Sprintf(operationPrefix+"cleanup.status.%d", poll)
				receipt, err = RunnerOperation(cleanup, req.Queue, req.Identity.JobID, cancel, limit)
			}
			stopped = err == nil && receipt.ProcessStopped
		}
		if err := workflow.ExecuteActivity(cleanup, "cloud.RecordOutcome", req.Identity.JobID, result.Compute, result.Publication, stopped).Get(cleanup, nil); err != nil {
			result.Cleanup = "unknown"
			result.Error += "; outcome persistence failed: " + err.Error()
			return
		}
		if stopped && preparedMounts {
			unmount := req.Command
			unmount.Action = "unmount"
			unmount.OperationID = operationPrefix + "cleanup.unmount"
			if _, err := RunnerOperation(cleanup, req.Queue, req.Identity.JobID, unmount, limit); err != nil {
				result.Cleanup = "pending"
				result.Error += "; mount cleanup unconfirmed: " + err.Error()
				return
			}
		}
		for workflow.Now(cleanup).Before(limit) {
			var state State
			if err := workflow.ExecuteActivity(cleanup, "cloud.Cleanup", req.Identity.JobID).Get(cleanup, &state); err != nil {
				result.Cleanup = "pending"
				result.Error += "; resource cleanup pending: " + err.Error()
				return
			}
			result.State = state
			result.Cleanup = state.CleanupOutcome
			status.Cleanup = result.Cleanup
			if result.Cleanup == "complete" || result.Cleanup == "retained" {
				status.Phase = "terminal"
				return
			}
			if err := workflow.Sleep(cleanup, 5*time.Second); err != nil {
				break
			}
		}
		result.Cleanup = "pending"
	}()
	if req.CollectionID != "" {
		if !identifier.MatchString(req.CollectionID) || len(req.CollectionID) > 32 {
			return result, fmt.Errorf("invalid collection identity")
		}
		if err := workflow.ExecuteActivity(act, "cloud.BeginCollection", req.Identity.JobID, req.CollectionID).Get(ctx, &result.State); err != nil {
			return result, err
		}
		if result.State.Request != req.Identity || result.State.ComputeOutcome != "succeeded" || result.State.CleanupOutcome != "retained" || !result.State.ProcessStopped {
			return result, fmt.Errorf("collection requires retained resources and confirmed successful computation")
		}
		until, e := time.Parse(time.RFC3339, result.State.RetainUntil)
		if e != nil || !workflow.Now(ctx).Before(until) {
			return result, fmt.Errorf("retention expired; reconcile resources before a new attempt")
		}
		acquired = true
		result.Compute = "succeeded"
	} else {
		var probe Probe
		if err := workflow.ExecuteActivity(act, "cloud.Probe", req.Identity).Get(ctx, &probe); err != nil {
			return result, err
		}
		if !probe.Ready {
			return result, temporal.NewNonRetryableApplicationError("AWS preflight blocked", "Configuration", nil, probe.Blockers)
		}
		bootstrap := workflow.Now(ctx).Add(time.Duration(req.BootstrapSeconds) * time.Second)
		for {
			// Intent may exist even when an activity loses its response. Cleanup must
			// attempt reconciliation after any acquisition dispatch, not just success.
			acquired = true
			if err := workflow.ExecuteActivity(act, "cloud.Acquire", req.Identity).Get(ctx, &result.State); err != nil {
				return result, err
			}
			if result.State.Phase == "preparing_storage" {
				break
			}
			if !workflow.Now(ctx).Before(bootstrap) {
				return result, fmt.Errorf("compute bootstrap deadline exceeded")
			}
			if err := workflow.Sleep(ctx, 5*time.Second); err != nil {
				return result, err
			}
		}
		status.Phase = "preparing_storage"
		for {
			if err := workflow.ExecuteActivity(act, "cloud.PrepareStorage", req.Identity.JobID).Get(ctx, &result.State); err != nil {
				return result, err
			}
			if result.State.Phase == "staging" {
				break
			}
			if !workflow.Now(ctx).Before(bootstrap) {
				return result, fmt.Errorf("storage bootstrap deadline exceeded")
			}
			if err := workflow.Sleep(ctx, 5*time.Second); err != nil {
				return result, err
			}
		}
	}
	prepare := req.Command
	prepare.Action = "prepare"
	prepare.OperationID = operationPrefix + "prepare"
	preparedMounts = true
	if _, err := RunnerOperation(ctx, req.Queue, req.Identity.JobID, prepare, workflow.Now(ctx).Add(time.Duration(req.BootstrapSeconds)*time.Second)); err != nil {
		return result, err
	}
	if req.CollectionID == "" {
		status.Phase = "staging"
		for _, input := range req.Inputs {
			activeCommand = input
			started = true
			stopped = false
			receipt, err := waitTransfer(ctx, req, input, workflow.Now(ctx).Add(time.Duration(req.CollectionSeconds)*time.Second), &activeCommand)
			stopped = receipt.ProcessStopped
			if err != nil {
				return result, err
			}
		}
		status.Phase = "validating_runtime"
		runtime := req.Command
		runtime.Action = "runtime"
		runtime.OperationID = "runtime"
		if _, err := RunnerOperation(ctx, req.Queue, req.Identity.JobID, runtime, workflow.Now(ctx).Add(time.Minute)); err != nil {
			return result, err
		}
		status.Phase = "running"
		result.Compute = "running"
		started = true
		stopped = false
		activeCommand = req.Command
		scientific := req.Command
		scientific.Action = "start"
		scientific.OperationID = "science.start"
		var err error
		result.Runner, err = waitCommand(ctx, req, scientific, workflow.Now(ctx).Add(time.Duration(req.JobSeconds)*time.Second))
		stopped = result.Runner.ProcessStopped
		if err != nil {
			result.Compute = result.Runner.ProcessState
			if result.Compute == "" {
				result.Compute = "unknown"
			}
			return result, err
		}
		result.Compute = "succeeded"
	}
	status.Compute = result.Compute
	status.Phase = "publishing"
	result.Publication = "running"
	for _, output := range req.Outputs {
		if req.CollectionID != "" {
			output.CommandID += "." + req.CollectionID
			output.OperationID += "." + req.CollectionID
		}
		activeCommand = output
		started = true
		stopped = false
		receipt, err := waitTransfer(ctx, req, output, workflow.Now(ctx).Add(time.Duration(req.CollectionSeconds)*time.Second), &activeCommand)
		stopped = receipt.ProcessStopped
		if err != nil {
			result.Publication = "failed"
			return result, err
		}
		result.Publications = append(result.Publications, receipt)
	}
	result.Publication = "succeeded"
	status.Publication = result.Publication
	return result, nil
}
