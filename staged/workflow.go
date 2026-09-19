package staged

import (
	"fmt"
	"regexp"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

type StageState struct {
	State            string `json:"state"`
	TaskID           string `json:"task_id,omitempty"`
	JobID            string `json:"job_id,omitempty"`
	ExitCode         int    `json:"exit_code,omitempty"`
	Files            int    `json:"files,omitempty"`
	FilesTransferred int    `json:"files_transferred,omitempty"`
	BytesTransferred int64  `json:"bytes_transferred,omitempty"`
	Faults           int    `json:"faults,omitempty"`
}

type Status struct {
	WorkflowStatus string                `json:"workflow_status"`
	CurrentStage   string                `json:"current_stage"`
	Stages         map[string]StageState `json:"stages"`
	Artifacts      map[string]string     `json:"artifacts"`
}

type TransferStatus struct {
	TaskID           string `json:"task_id"`
	Status           string `json:"status"`
	Files            int    `json:"files"`
	FilesTransferred int    `json:"files_transferred"`
	BytesTransferred int64  `json:"bytes_transferred"`
	Faults           int    `json:"faults"`
	FatalError       string `json:"fatal_error"`
}

type SlurmStatus struct {
	JobID    string `json:"job_id"`
	Status   string `json:"status"`
	ExitCode int    `json:"exit_code"`
	Logs     string `json:"logs"`
}

type GPUResult struct {
	Success         bool   `json:"success"`
	JobID           string `json:"job_id"`
	Logs            string `json:"logs"`
	OutputItemCount int    `json:"output_item_count"`
	LocalOutputDir  string `json:"local_output_dir"`
}

type Result struct {
	Success         bool              `json:"success"`
	TransferTaskIDs map[string]string `json:"transfer_task_ids"`
	SlurmJobID      *int              `json:"slurm_job_id"`
	SlurmStatus     string            `json:"slurm_status"`
	GPUResult       *GPUResult        `json:"gpu_result"`
}

var dockerUnsafeID = regexp.MustCompile(`[^A-Za-z0-9]`)

func gpuContainerID(workflowID string) string {
	safe := dockerUnsafeID.ReplaceAllString(workflowID, "_")
	if len(safe) > 48 {
		safe = safe[len(safe)-48:]
	}
	return "rdjob_" + safe
}

func activityContext(ctx workflow.Context, queue string, timeout time.Duration, attempts int) workflow.Context {
	return workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue:           queue,
		StartToCloseTimeout: timeout,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: int32(attempts)},
	})
}

func sleepInterval(seconds int) time.Duration {
	if seconds <= 0 {
		seconds = 15
	}
	return time.Duration(seconds) * time.Second
}

func transferTimeout(seconds int) time.Duration {
	if seconds <= 0 {
		seconds = 86400
	}
	return time.Duration(seconds) * time.Second
}

func gpuTimeout(seconds int) time.Duration {
	if seconds <= 0 {
		seconds = 7200
	}
	return time.Duration(seconds) * time.Second
}

func Run(ctx workflow.Context, req Request) (result Result, err error) {
	req.Normalize()
	if err = req.Validate(); err != nil {
		return result, err
	}
	status := Status{WorkflowStatus: "Started", CurrentStage: "initializing", Stages: map[string]StageState{}, Artifacts: map[string]string{}}
	if err = workflow.SetQueryHandler(ctx, "get_status", func() (Status, error) { return status, nil }); err != nil {
		return result, err
	}
	result = Result{Success: false, TransferTaskIDs: map[string]string{}, SlurmStatus: "SKIPPED"}
	var slurmJobID, gpuJobID string
	defer func() {
		if err == nil {
			return
		}
		status.WorkflowStatus = "Failed"
		cleanupCtx, _ := workflow.NewDisconnectedContext(ctx)
		if slurmJobID != "" {
			c := activityContext(cleanupCtx, req.Slurm.TaskQueue, 2*time.Minute, 2)
			if cleanupErr := workflow.ExecuteActivity(c, "staged.CancelSlurm", slurmJobID).Get(c, nil); cleanupErr != nil {
				workflow.GetLogger(ctx).Error("Slurm cancellation failed", "job_id", slurmJobID, "error", cleanupErr)
			}
		}
		if gpuJobID != "" {
			c := activityContext(cleanupCtx, req.GPU.TaskQueue, 2*time.Minute, 2)
			if cleanupErr := workflow.ExecuteActivity(c, "staged.CleanupGPU", gpuJobID).Get(c, nil); cleanupErr != nil {
				workflow.GetLogger(ctx).Error("GPU cleanup failed", "job_id", gpuJobID, "error", cleanupErr)
			}
		}
	}()

	runTransfer := func(name string, t *Transfer) error {
		status.CurrentStage = name
		status.Stages[name] = StageState{State: "SUBMITTING"}
		c := activityContext(ctx, req.GlobusTaskQueue, 10*time.Minute, 3)
		var taskID string
		if e := workflow.ExecuteActivity(c, "staged.SubmitTransfer", *t).Get(c, &taskID); e != nil {
			return e
		}
		status.Artifacts[name+"_task_id"] = taskID
		status.Stages[name] = StageState{State: "RUNNING", TaskID: taskID}
		deadline := workflow.Now(ctx).Add(transferTimeout(t.TimeoutSeconds))
		for {
			var poll TransferStatus
			c = activityContext(ctx, req.GlobusTaskQueue, 2*time.Minute, 5)
			if e := workflow.ExecuteActivity(c, "staged.PollTransfer", taskID).Get(c, &poll); e != nil {
				return e
			}
			status.Stages[name] = StageState{State: poll.Status, TaskID: taskID, Files: poll.Files, FilesTransferred: poll.FilesTransferred, BytesTransferred: poll.BytesTransferred, Faults: poll.Faults}
			switch poll.Status {
			case "SUCCEEDED":
				result.TransferTaskIDs[name] = taskID
				return nil
			case "FAILED", "CANCELED", "EXPIRED":
				return fmt.Errorf("Globus %s failed: %s faults=%d fatal=%s", name, poll.Status, poll.Faults, poll.FatalError)
			}
			if !workflow.Now(ctx).Before(deadline) {
				return fmt.Errorf("Globus %s timed out", name)
			}
			if e := workflow.Sleep(ctx, sleepInterval(t.PollIntervalSeconds)); e != nil {
				return e
			}
		}
	}

	if req.StageIn != nil {
		if err = runTransfer("stage_in", req.StageIn); err != nil {
			return result, err
		}
	}
	if req.Slurm != nil {
		status.CurrentStage = "slurm"
		status.Stages["slurm"] = StageState{State: "SUBMITTING"}
		c := activityContext(ctx, req.Slurm.TaskQueue, 10*time.Minute, 1)
		if err = workflow.ExecuteActivity(c, "staged.SubmitSlurm", req.Slurm.Job).Get(c, &slurmJobID); err != nil {
			return result, err
		}
		status.Artifacts["slurm_job_id"] = slurmJobID
		status.Stages["slurm"] = StageState{State: "RUNNING", JobID: slurmJobID}
		deadline := workflow.Now(ctx).Add(transferTimeout(req.Slurm.TimeoutSeconds))
		for {
			var poll SlurmStatus
			c = activityContext(ctx, req.Slurm.TaskQueue, 5*time.Minute, 5)
			if err = workflow.ExecuteActivity(c, "staged.PollSlurm", slurmJobID).Get(c, &poll); err != nil {
				return result, err
			}
			if poll.Status == "COMPLETED" || poll.Status == "FAILED" || poll.Status == "CANCELLED" || poll.Status == "TIMEOUT" || poll.Status == "OUT_OF_MEMORY" {
				status.Stages["slurm"] = StageState{State: poll.Status, JobID: slurmJobID, ExitCode: poll.ExitCode}
				if poll.Status != "COMPLETED" || poll.ExitCode != 0 {
					return result, fmt.Errorf("Slurm job %s ended %s exit=%d logs=%s", slurmJobID, poll.Status, poll.ExitCode, poll.Logs)
				}
				result.SlurmStatus = poll.Status
				break
			}
			if !workflow.Now(ctx).Before(deadline) {
				return result, fmt.Errorf("Slurm job %s timed out", slurmJobID)
			}
			if err = workflow.Sleep(ctx, sleepInterval(req.Slurm.PollIntervalSeconds)); err != nil {
				return result, err
			}
		}
		var id int
		_, _ = fmt.Sscan(slurmJobID, &id)
		result.SlurmJobID = &id
		slurmJobID = ""
	} else {
		status.Stages["slurm"] = StageState{State: "SKIPPED"}
	}
	if req.StageBack != nil {
		if err = runTransfer("stage_back", req.StageBack); err != nil {
			return result, err
		}
	}
	if req.GPU != nil {
		status.CurrentStage = "gpu"
		status.Stages["gpu"] = StageState{State: "PREFLIGHT"}
		c := activityContext(ctx, req.GPU.TaskQueue, time.Minute, 0)
		c = workflow.WithActivityOptions(c, workflow.ActivityOptions{TaskQueue: req.GPU.TaskQueue, StartToCloseTimeout: time.Minute, ScheduleToCloseTimeout: gpuTimeout(req.GPU.Job.GPUWaitTimeoutSeconds), RetryPolicy: &temporal.RetryPolicy{InitialInterval: 15 * time.Second, MaximumInterval: time.Minute}})
		if err = workflow.ExecuteActivity(c, "staged.PreflightGPU", req.GPU.Job).Get(c, nil); err != nil {
			return result, err
		}
		gpuJobID = gpuContainerID(req.WorkflowID)
		status.Stages["gpu"] = StageState{State: "RUNNING", JobID: gpuJobID}
		c = activityContext(ctx, req.GPU.TaskQueue, time.Minute, 2)
		if err = workflow.ExecuteActivity(c, "staged.SetupGPU", gpuJobID).Get(c, nil); err != nil {
			return result, err
		}
		c = activityContext(ctx, req.GPU.TaskQueue, time.Hour, 2)
		if err = workflow.ExecuteActivity(c, "staged.UploadGPU", gpuJobID, req.GPU.Job).Get(c, nil); err != nil {
			return result, err
		}
		c = activityContext(ctx, req.GPU.TaskQueue, gpuTimeout(req.GPU.Job.TimeoutSeconds)+time.Minute, 1)
		var logs string
		if err = workflow.ExecuteActivity(c, "staged.RunGPU", gpuJobID, req.GPU.Job).Get(c, &logs); err != nil {
			return result, err
		}
		c = activityContext(ctx, req.GPU.TaskQueue, time.Hour, 2)
		var count int
		if err = workflow.ExecuteActivity(c, "staged.DownloadGPU", gpuJobID, req.GPU.Job.LocalOutputDir).Get(c, &count); err != nil {
			return result, err
		}
		result.GPUResult = &GPUResult{Success: true, JobID: gpuJobID, Logs: logs, OutputItemCount: count, LocalOutputDir: req.GPU.Job.LocalOutputDir}
		status.Stages["gpu"] = StageState{State: "SUCCEEDED", JobID: gpuJobID}
		if req.GPU.Job.Cleanup == nil || *req.GPU.Job.Cleanup {
			c = activityContext(ctx, req.GPU.TaskQueue, time.Minute, 2)
			if err = workflow.ExecuteActivity(c, "staged.CleanupGPU", gpuJobID).Get(c, nil); err != nil {
				return result, err
			}
		}
		gpuJobID = ""
	} else {
		status.Stages["gpu"] = StageState{State: "SKIPPED"}
	}
	if req.Publish != nil {
		if err = runTransfer("publish", req.Publish); err != nil {
			return result, err
		}
	}
	status.WorkflowStatus = "Finished"
	status.CurrentStage = "complete"
	result.Success = true
	return result, nil
}
