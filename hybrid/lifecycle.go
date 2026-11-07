package hybrid

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"go-scheduler/cloudexec"
	"go-scheduler/durable"
	"go-scheduler/transfer"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

type Inspection struct {
	Stage     Stage  `json:"stage"`
	Operation string `json:"operation"`
	Stream    string `json:"stream"`
	Cursor    int64  `json:"cursor"`
	Apply     bool   `json:"apply"`
}

func Inspect(ctx workflow.Context, req Inspection) (json.RawMessage, error) {
	stage := req.Stage
	if req.Operation == "reconcile" {
		if stage.Cloud == nil {
			return json.RawMessage(`{"cleanup":"not_required"}`), nil
		}
		var state cloudexec.State
		err := workflow.ExecuteActivity(act(ctx, stage.Site.Queue, 2*time.Minute), "cloud.Reconcile", stage.JobID, req.Apply).Get(ctx, &state)
		b, _ := json.Marshal(state)
		return b, err
	}
	if req.Operation != "logs" || req.Cursor < 0 || (req.Stream != "stdout" && req.Stream != "stderr") {
		return nil, fmt.Errorf("invalid inspection")
	}
	if stage.Cloud != nil {
		var terminal cloudexec.StageResult
		if e := workflow.ExecuteActivity(act(ctx, stage.Site.Queue, time.Minute), "cloud.ReadStageResult", stage.JobID).Get(ctx, &terminal); e == nil && terminal.Compute != "running" {
			var data struct {
				Tails map[string]json.RawMessage `json:"log_tails"`
			}
			if json.Unmarshal(terminal.Runner.Data, &data) == nil && len(data.Tails[req.Stream]) > 0 {
				return data.Tails[req.Stream], nil
			}
		}
		r := stage.Cloud.Command
		r.Action = "logs"
		r.OperationID = transferID(workflow.GetInfo(ctx).WorkflowExecution.ID, "logs")
		r.Stream = req.Stream
		r.Cursor = req.Cursor
		result, err := cloudexec.RunnerOperation(ctx, stage.Site.Queue, stage.JobID, r, workflow.Now(ctx).Add(90*time.Second))
		return result.Data, err
	}
	var result json.RawMessage
	err := workflow.ExecuteActivity(act(ctx, stage.Site.Queue, time.Minute), "hybrid.Logs", stage, req.Stream, req.Cursor).Get(ctx, &result)
	return result, err
}
func (a *Activities) Logs(ctx context.Context, stage Stage, stream string, cursor int64) (json.RawMessage, error) {
	if e := a.check(stage); e != nil {
		return nil, e
	}
	if cursor < 0 || (stream != "stdout" && stream != "stderr") {
		return nil, fmt.Errorf("invalid log cursor")
	}
	root, e := os.OpenRoot(filepath.Join(a.Config.JournalRoot, "runner", stage.JobID, "science"))
	if e != nil {
		return nil, e
	}
	defer root.Close()
	f, e := root.Open(stream + ".log")
	if e != nil {
		return nil, e
	}
	defer f.Close()
	if _, e = f.Seek(cursor, 0); e != nil {
		return nil, e
	}
	b := make([]byte, 4096)
	n, e := f.Read(b)
	if n == 0 && e != nil && e.Error() != "EOF" {
		return nil, e
	}
	raw, e := json.Marshal(map[string]any{"stream": stream, "cursor": cursor, "next_cursor": cursor + int64(n), "text": string(b[:n])})
	return raw, e
}

type Collection struct {
	Prepared Prepared `json:"prepared"`
	Prior    Result   `json:"prior"`
	ID       string   `json:"collection_id"`
}

func (a *Activities) CollectionLease(ctx context.Context, id, owner string, release bool) error {
	return a.Store.With(ctx, "hybrid-collection/"+id, func(e *durable.Entry) error {
		var active string
		if err := e.Read(&active); err != nil && !os.IsNotExist(err) {
			return err
		}
		if active != "" && active != owner {
			return fmt.Errorf("another collection owns this workflow; reconcile its outcome first")
		}
		if release {
			return e.Write("")
		}
		return e.Write(owner)
	})
}

// Collect retries publication from retained successful computation. It never
// starts scientific commands or pending downstream nodes; those need an explicit
// reviewed continuation/new attempt.
func Collect(ctx workflow.Context, req Collection) (result Result, err error) {
	result = req.Prior
	result.State = "collecting"
	if e := workflow.SetQueryHandler(ctx, "status", func() (Result, error) { return result, nil }); e != nil {
		return result, e
	}
	queue := workflow.GetInfo(ctx).TaskQueueName
	activityCtx := act(ctx, queue, time.Minute)
	if err = workflow.ExecuteActivity(activityCtx, "hybrid.CollectionLease", result.WorkflowID, req.ID, false).Get(ctx, nil); err != nil {
		return result, err
	}
	defer func() {
		detached, _ := workflow.NewDisconnectedContext(ctx)
		a := act(detached, queue, time.Minute)
		if e := workflow.ExecuteActivity(a, "hybrid.PersistResult", result.WorkflowID, result).Get(detached, nil); e != nil {
			err = e
			return
		}
		if e := workflow.ExecuteActivity(a, "hybrid.CollectionLease", result.WorkflowID, req.ID, true).Get(detached, nil); e != nil {
			err = e
		}
	}()
	for _, id := range req.Prepared.Order {
		prior, ok := result.Nodes[id]
		if !ok || prior.Publication == "succeeded" {
			continue
		}
		if prior.Compute != "succeeded" {
			return result, fmt.Errorf("stage %s requires an explicit new computation attempt", id)
		}
		stage := req.Prepared.Stages[id]
		if stage.Cloud != nil {
			stage.Cloud.CollectionID = req.ID
			child := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{WorkflowID: stage.JobID + "-collect-" + req.ID, RetryPolicy: &temporal.RetryPolicy{MaximumAttempts: 1}, WaitForCancellation: true})
			var node NodeResult
			if e := workflow.ExecuteChildWorkflow(child, NodeWorkflow, stage).Get(ctx, &node); e != nil {
				return result, e
			}
			result.Nodes[id] = node
		} else {
			for _, out := range stage.Outputs {
				var artifact transfer.Artifact
				e := workflow.ExecuteActivity(act(ctx, stage.Site.Queue, 24*time.Hour), "hybrid.Publish", stage, out).Get(ctx, &artifact)
				if e != nil {
					prior.Error = e.Error()
					result.Nodes[id] = prior
					return result, e
				}
				if prior.Artifacts == nil {
					prior.Artifacts = map[string]transfer.Artifact{}
				}
				prior.Artifacts[out.Key] = artifact
			}
			prior.State = "succeeded"
			prior.Publication = "succeeded"
			prior.Error = ""
			result.Nodes[id] = prior
		}
	}
	req.Prepared.Resume = &result
	return Run(ctx, req.Prepared)
}
