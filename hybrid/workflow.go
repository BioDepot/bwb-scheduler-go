package hybrid

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"path"
	"strings"
	"time"

	"go-scheduler/cloudexec"
	"go-scheduler/transfer"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

func act(ctx workflow.Context, queue string, timeout time.Duration) workflow.Context {
	return workflow.WithActivityOptions(ctx, workflow.ActivityOptions{TaskQueue: queue, StartToCloseTimeout: timeout, ScheduleToCloseTimeout: timeout * 3, WaitForCancellation: true, RetryPolicy: &temporal.RetryPolicy{MaximumAttempts: 3, InitialInterval: 2 * time.Second, MaximumInterval: 20 * time.Second, NonRetryableErrorTypes: []string{"Configuration", "Conflict"}}})
}
func transferID(parts ...string) string {
	raw, _ := json.Marshal(parts)
	return fmt.Sprintf("transfer-%x", sha256.Sum256(raw))
}
func transferCommand(stage Stage, r transfer.Request, publish bool) cloudexec.RunnerRequest {
	command := transfer.Command{Request: r, Root: stage.Site.Root, JournalRoot: "/var/lib/workbench-runner/transfers", Region: stage.Site.Region, InputPrefixes: stage.CloudStorage.Inputs, OutputPrefixes: []string{stage.CloudStorage.Output}, Encryption: stage.CloudStorage.Encryption, KMSKey: stage.CloudStorage.KMS, Publish: publish}
	if !publish && !strings.HasPrefix(r.Source.URI, "s3://") {
		for _, root := range stage.StorageRoots {
			if transfer.Within(r.Source.URI, root) {
				command.Root = root
				break
			}
		}
	}
	raw, _ := json.Marshal(command)
	return cloudexec.RunnerRequest{OperationID: r.ID, Action: "start", CommandID: r.ID, WorkRoot: stage.WorkRoot, TimeoutSeconds: stage.Cloud.CollectionSeconds, Argv: []string{"/opt/workbench/bwbScheduler", "artifact-transfer", "--request-base64", base64.StdEncoding.EncodeToString(raw)}}
}
func NodeWorkflow(ctx workflow.Context, stage Stage) (result NodeResult, err error) {
	result = NodeResult{State: "running", Compute: "not_started", Publication: "not_started", Cleanup: "not_required", Artifacts: map[string]transfer.Artifact{}}
	for _, source := range stage.SourceUploads {
		var receipt transfer.Receipt
		if e := workflow.ExecuteActivity(act(ctx, source.Queue, 24*time.Hour), "hybrid.Transfer", *source.Stage, source.Request).Get(ctx, &receipt); e != nil || !receipt.Verified {
			result.State = "failed"
			result.Error = fmt.Sprintf("source staging failed: %v", e)
			return result, nil
		}
	}
	if stage.Cloud != nil {
		req := *stage.Cloud
		for _, input := range stage.EBSUploads {
			req.Inputs = append(req.Inputs, transferCommand(stage, input, false))
		}
		for _, input := range stage.Inputs {
			req.Inputs = append(req.Inputs, transferCommand(stage, input, false))
		}
		for _, out := range stage.Outputs {
			r := transfer.Request{ID: transferID(stage.JobID, out.Key), AttemptID: stage.JobID, SourceEndpoint: stage.Node.Key, DestinationEndpoint: "s3", Source: transfer.Artifact{URI: path.Join(stage.WorkRoot, out.Path)}, Destination: out.Destination}
			req.Outputs = append(req.Outputs, transferCommand(stage, r, true))
		}
		options := workflow.ChildWorkflowOptions{WorkflowID: stage.JobID + "-ec2" + req.CollectionID, TaskQueue: stage.Site.Queue, WorkflowExecutionTimeout: time.Duration(req.BootstrapSeconds+req.JobSeconds+req.CollectionSeconds*(len(req.Inputs)+len(req.Outputs))+1200) * time.Second, RetryPolicy: &temporal.RetryPolicy{MaximumAttempts: 1}, WaitForCancellation: true}
		child := workflow.WithChildOptions(ctx, options)
		var cloudResult cloudexec.StageResult
		err = workflow.ExecuteChildWorkflow(child, cloudexec.StageWorkflow, req).Get(ctx, &cloudResult)
		if err != nil {
			detached, _ := workflow.NewDisconnectedContext(ctx)
			_ = workflow.ExecuteActivity(act(detached, stage.Site.Queue, time.Minute), "cloud.ReadStageResult", stage.JobID).Get(detached, &cloudResult)
		}
		result.Compute, result.Publication, result.Cleanup = cloudResult.Compute, cloudResult.Publication, cloudResult.Cleanup
		if err != nil {
			result.State = "failed"
			result.Error = err.Error()
			return result, nil
		}
		for i, pub := range cloudResult.Publications {
			var data struct {
				Result transfer.Receipt `json:"command_result"`
			}
			if e := json.Unmarshal(pub.Data, &data); e != nil || !data.Result.Verified {
				result.State = "failed"
				result.Error = "publication has no verified artifact receipt"
				return result, nil
			}
			result.Artifacts[stage.Outputs[i].Key] = data.Result.Destination
		}
	} else {
		c := act(ctx, stage.Site.Queue, 2*time.Minute)
		if err = workflow.ExecuteActivity(c, "hybrid.PrepareWorkspace", stage).Get(ctx, nil); err != nil {
			result.State = "failed"
			result.Error = err.Error()
			return result, nil
		}
		for _, input := range stage.Inputs {
			var receipt transfer.Receipt
			if err = workflow.ExecuteActivity(act(ctx, stage.Site.Queue, 24*time.Hour), "hybrid.Transfer", stage, input).Get(ctx, &receipt); err != nil || !receipt.Verified {
				result.State = "failed"
				result.Error = fmt.Sprintf("input transfer not verified: %v", err)
				return result, nil
			}
		}
		c = act(ctx, stage.Site.Queue, time.Duration(stage.Command.TimeoutSeconds+180)*time.Second)
		c = workflow.WithActivityOptions(c, workflow.ActivityOptions{TaskQueue: stage.Site.Queue, StartToCloseTimeout: time.Duration(stage.Command.TimeoutSeconds+180) * time.Second, HeartbeatTimeout: 30 * time.Second, WaitForCancellation: true, RetryPolicy: &temporal.RetryPolicy{MaximumAttempts: 3}})
		var command LocalCommandResult
		err = workflow.ExecuteActivity(c, "hybrid.RunLocal", stage).Get(ctx, &command)
		result.Compute = command.State
		if err != nil || command.State != "succeeded" || !command.Stopped {
			result.State = "failed"
			result.Error = fmt.Sprintf("computation unsuccessful: %v", err)
			return result, nil
		}
		result.Publication = "running"
		for _, out := range stage.Outputs {
			var artifact transfer.Artifact
			if err = workflow.ExecuteActivity(act(ctx, stage.Site.Queue, 24*time.Hour), "hybrid.Publish", stage, out).Get(ctx, &artifact); err != nil {
				result.State = "collection_failed"
				result.Publication = "failed"
				result.Error = err.Error()
				return result, nil
			}
			result.Artifacts[out.Key] = artifact
		}
		result.Publication = "succeeded"
	}
	if result.Compute == "succeeded" && result.Publication == "succeeded" {
		result.State = "succeeded"
	} else {
		result.State = "failed"
	}
	return result, nil
}
func Run(ctx workflow.Context, p Prepared) (result Result, err error) {
	result = Result{State: "running", Nodes: map[string]NodeResult{}, Owner: p.Plan.Identity.Owner, WorkflowID: p.Plan.Identity.WorkflowID}
	if p.Resume != nil {
		result.Nodes = p.Resume.Nodes
	}
	if len(p.Plan.Checkpoints) > 0 {
		result.Checkpoints = map[string]CheckpointResult{}
		if p.Resume != nil && p.Resume.Checkpoints != nil {
			result.Checkpoints = p.Resume.Checkpoints
		}
	}
	if err = workflow.SetQueryHandler(ctx, "status", func() (Result, error) { return result, nil }); err != nil {
		return result, err
	}
	defer func() {
		if err != nil {
			result.State = "failed"
			if temporal.IsCanceledError(err) {
				result.State = "cancelled"
			}
		}
		detached, _ := workflow.NewDisconnectedContext(ctx)
		persist := act(detached, workflow.GetInfo(ctx).TaskQueueName, time.Minute)
		if e := workflow.ExecuteActivity(persist, "hybrid.PersistResult", result.WorkflowID, result).Get(detached, nil); e != nil {
			result.State = "evidence_pending"
			if err == nil {
				err = e
			}
		}
	}()
	routes := map[string]Route{}
	for _, r := range p.Plan.Storage.Routes {
		routes[r.ID] = r
	}
	artifacts := map[string]Artifact{}
	for _, a := range p.Plan.Storage.Artifacts {
		artifacts[a.ID] = a
	}
	pending := map[string]bool{}
	for _, id := range p.Order {
		if result.Nodes[id].State != "succeeded" {
			pending[id] = true
		}
	}
	running := map[string]workflow.ChildWorkflowFuture{}
	counts := map[string]int{}
	cancelCtx, cancel := workflow.WithCancel(ctx)
	defer cancel()
	var decisions workflow.ReceiveChannel
	if len(p.Plan.Checkpoints) > 0 {
		decisions = workflow.GetSignalChannel(ctx, CheckpointSignal)
	}
	timers := map[string]workflow.Future{}
	var receivedDecision *CheckpointDecision
	timerCancels := map[string]workflow.CancelFunc{}
	defer func() {
		for _, gate := range p.Plan.Checkpoints {
			if stop := timerCancels[gate.ID]; stop != nil {
				stop()
			}
		}
	}()
	for len(pending) > 0 || len(running) > 0 {
		waiting := false
		if decisions != nil {
			changed := false
			if receivedDecision != nil {
				acceptCheckpointDecision(ctx, &result, *receivedDecision)
				receivedDecision = nil
				changed = true
			}
			var decision CheckpointDecision
			for decisions.ReceiveAsync(&decision) {
				acceptCheckpointDecision(ctx, &result, decision)
				changed = true
			}
			changed = advanceCheckpoints(ctx, p, &result) || changed
			for _, gate := range p.Plan.Checkpoints {
				state := result.Checkpoints[gate.ID]
				if state.State == "waiting_for_approval" {
					waiting = true
					if timers[gate.ID] == nil {
						timerCtx, stop := workflow.WithCancel(ctx)
						timerCancels[gate.ID] = stop
						timers[gate.ID] = workflow.NewTimer(timerCtx, state.ExpiresAt.Sub(workflow.Now(ctx)))
					}
				} else if stop := timerCancels[gate.ID]; stop != nil {
					stop()
					delete(timerCancels, gate.ID)
				}
			}
			result.State = "running"
			if waiting {
				result.State = "waiting_for_approval"
			}
			gateErr := checkpointFailure(result)
			if changed && gateErr == nil {
				gateErr = workflow.ExecuteActivity(act(ctx, workflow.GetInfo(ctx).TaskQueueName, time.Minute), "hybrid.PersistResult", result.WorkflowID, result).Get(ctx, nil)
			}
			if ctx.Err() != nil {
				gateErr = ctx.Err()
			}
			if gateErr != nil {
				cancel()
				detached, _ := workflow.NewDisconnectedContext(ctx)
				for _, id := range p.Order {
					if future, ok := running[id]; ok {
						var node NodeResult
						if e := future.Get(detached, &node); e != nil {
							node = NodeResult{State: "cancelled", Error: e.Error()}
						}
						result.Nodes[id] = node
					}
				}
				return result, gateErr
			}
		}
		for _, id := range p.Order {
			if !pending[id] {
				continue
			}
			stage := p.Stages[id]
			ready := true
			for _, dep := range stage.Node.Depends {
				prior, ok := result.Nodes[dep]
				if !ok {
					ready = false
					break
				}
				if prior.State != "succeeded" {
					return result, fmt.Errorf("dependency %s failed", dep)
				}
			}
			if !ready || checkpointBlocks(p, result, id) || counts[stage.Node.Key] >= stage.Site.MaxJobs {
				continue
			}
			for _, input := range p.Plan.Storage.Inputs {
				if input.NodeID != id {
					continue
				}
				source := artifacts[input.ArtifactID].Source
				route := routes[input.RouteID]
				var value transfer.Artifact
				switch source.Kind {
				case "s3":
					value = transfer.Artifact{URI: source.URI, SHA256: source.Hash, Size: source.Size, VersionID: source.Version}
				case "stage_output":
					var ok bool
					value, ok = result.Nodes[source.Node].Artifacts[input.ArtifactID+"."+route.ID]
					if !ok {
						return result, fmt.Errorf("upstream publication missing for %s", input.ArtifactID)
					}
				case "server_file":
					var sourceStage Stage
					for _, nodeID := range p.Order {
						candidate := p.Stages[nodeID]
						if candidate.Node.Key == source.Executor {
							sourceStage = candidate
							break
						}
					}
					upload := transfer.Request{ID: transferID(stage.JobID, input.ArtifactID, "source"), AttemptID: stage.JobID, SourceEndpoint: source.Executor, DestinationEndpoint: "s3", Source: transfer.Artifact{URI: source.Path, SHA256: source.Hash, Size: source.Size}, Destination: route.Prefix + stage.JobID + "/" + input.ArtifactID}
					sourceCopy := sourceStage
					stage.SourceUploads = append(stage.SourceUploads, StagedSource{Stage: &sourceCopy, Queue: route.Queue, Request: upload})
					value = transfer.Artifact{URI: upload.Destination, SHA256: source.Hash, Size: source.Size}
				case "ebs_path":
					upload := transfer.Request{ID: transferID(stage.JobID, input.ArtifactID, "ebs"), AttemptID: stage.JobID, SourceEndpoint: stage.Node.Key, DestinationEndpoint: "s3", Source: transfer.Artifact{URI: source.Path, SHA256: source.Hash, Size: source.Size}, Destination: route.Prefix + stage.JobID + "/" + input.ArtifactID}
					stage.EBSUploads = append(stage.EBSUploads, upload)
					value = transfer.Artifact{URI: upload.Destination, SHA256: source.Hash, Size: source.Size}
				default:
					return result, fmt.Errorf("unsupported artifact source")
				}
				stage.Inputs = append(stage.Inputs, transfer.Request{ID: transferID(stage.JobID, input.ArtifactID, "input"), AttemptID: stage.JobID, SourceEndpoint: "s3", DestinationEndpoint: stage.Node.Key, Source: value, Destination: path.Join(stage.WorkRoot, input.Path)})
			}
			child := workflow.WithChildOptions(cancelCtx, workflow.ChildWorkflowOptions{WorkflowID: stage.JobID, TaskQueue: workflow.GetInfo(ctx).TaskQueueName, WaitForCancellation: true, RetryPolicy: &temporal.RetryPolicy{MaximumAttempts: 1}})
			running[id] = workflow.ExecuteChildWorkflow(child, NodeWorkflow, stage)
			counts[stage.Node.Key]++
			delete(pending, id)
		}
		if len(running) == 0 && !waiting {
			return result, fmt.Errorf("DAG cannot make progress")
		}
		selector := workflow.NewSelector(ctx)
		if waiting {
			selector.AddReceive(decisions, func(channel workflow.ReceiveChannel, _ bool) {
				var decision CheckpointDecision
				channel.Receive(ctx, &decision)
				receivedDecision = &decision
			})
			for _, gate := range p.Plan.Checkpoints {
				if result.Checkpoints[gate.ID].State == "waiting_for_approval" {
					selector.AddFuture(timers[gate.ID], func(workflow.Future) {})
				}
			}
		}
		for _, id := range p.Order {
			future, ok := running[id]
			if !ok {
				continue
			}
			nodeID := id
			selector.AddFuture(future, func(f workflow.Future) {
				var node NodeResult
				e := f.Get(ctx, &node)
				if e != nil {
					node = NodeResult{State: "failed", Error: e.Error()}
				}
				result.Nodes[nodeID] = node
				delete(running, nodeID)
				counts[p.Stages[nodeID].Node.Key]--
			})
		}
		selector.Select(ctx)
		failed := ctx.Err() != nil
		for _, n := range result.Nodes {
			if n.State != "succeeded" {
				failed = true
			}
		}
		if failed {
			if decisions != nil {
				advanceCheckpoints(ctx, p, &result)
			}
			cancel()
			// Wait for every active child and its disconnected cleanup to finish.
			detached, _ := workflow.NewDisconnectedContext(ctx)
			for _, id := range p.Order {
				future, ok := running[id]
				if !ok {
					continue
				}
				var node NodeResult
				e := future.Get(detached, &node)
				if e != nil {
					node = NodeResult{State: "cancelled", Error: e.Error()}
				}
				result.Nodes[id] = node
			}
			if ctx.Err() != nil {
				return result, ctx.Err()
			}
			return result, fmt.Errorf("one or more hybrid stages failed")
		}
	}
	result.State = "succeeded"
	return result, nil
}
