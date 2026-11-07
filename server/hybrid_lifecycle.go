package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"go.temporal.io/api/serviceerror"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"go-scheduler/durable"
	"go-scheduler/hybrid"
	"go-scheduler/transfer"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
)

type hybridOperation struct {
	hybridIdentity
	NodeID       string `json:"node_id"`
	CollectionID string `json:"collection_id"`
	Apply        bool   `json:"apply"`
}

func (s *Server) handleHybridInspect(w http.ResponseWriter, r *http.Request) {
	if !s.hybridEnabled(w) {
		return
	}
	var op hybridOperation
	operation := strings.TrimPrefix(r.URL.Path, "/hybrid_")
	if operation == "logs" {
		if r.Method != http.MethodGet {
			writeError(w, 405, "GET required")
			return
		}
		op = hybridOperation{hybridIdentity: hybridIdentity{Owner: r.URL.Query().Get("owner_id"), WorkflowID: r.URL.Query().Get("workflow_id")}, NodeID: r.URL.Query().Get("node_id")}
	} else {
		if r.Method != http.MethodPost {
			writeError(w, 405, "POST required")
			return
		}
		if e := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&op); e != nil {
			writeError(w, 400, "invalid request")
			return
		}
	}
	record, e := s.ownedHybrid(r.Context(), op.hybridIdentity)
	if e != nil {
		writeError(w, 404, e.Error())
		return
	}
	if operation == "reconcile" && op.Apply {
		executions := map[string]string{op.WorkflowID: record.RunID}
		if record.ActiveCollection != "" {
			executions[record.ActiveCollection] = record.CollectionRunID
		}
		for workflowID, runID := range executions {
			description, err := s.temporalClient.DescribeWorkflowExecution(r.Context(), workflowID, runID)
			if err != nil {
				writeError(w, 409, "cannot verify workflow termination before reconciliation")
				return
			}
			if description.WorkflowExecutionInfo.Status == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
				writeError(w, 409, "cancel active workflow before applying reconciliation")
				return
			}
		}
	}
	stage, ok := record.Prepared.Stages[op.NodeID]
	if !ok {
		writeError(w, 400, "node_id required")
		return
	}
	cursor := int64(0)
	if value := r.URL.Query().Get("cursor"); value != "" {
		cursor, e = strconv.ParseInt(value, 10, 64)
		if e != nil || cursor < 0 {
			writeError(w, 400, "invalid log cursor")
			return
		}
	}
	stream := r.URL.Query().Get("stream")
	if stream == "" {
		stream = "stdout"
	}
	req := hybrid.Inspection{Stage: stage, Operation: operation, Stream: stream, Cursor: cursor, Apply: op.Apply}
	run, e := s.temporalClient.ExecuteWorkflow(r.Context(), client.StartWorkflowOptions{ID: "inspect-" + uuid.NewString(), TaskQueue: s.hybridConfig.Queue, WorkflowExecutionTimeout: 3 * time.Minute}, hybrid.Inspect, req)
	if e != nil {
		writeError(w, 502, e.Error())
		return
	}
	var result json.RawMessage
	if e = run.Get(r.Context(), &result); e != nil {
		writeError(w, 502, e.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(result)
}
func (s *Server) handleHybridCollect(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, 405, "POST required")
		return
	}
	if !s.hybridEnabled(w) {
		return
	}
	var op hybridOperation
	if e := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&op); e != nil || !regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]*$`).MatchString(op.CollectionID) || len(op.CollectionID) > 32 {
		writeError(w, 400, "a stable collection_id of at most 32 characters is required")
		return
	}
	record, e := s.ownedHybrid(r.Context(), op.hybridIdentity)
	if e != nil {
		writeError(w, 404, e.Error())
		return
	}

	store := durable.Store{Root: s.hybridConfig.JournalRoot}
	workflowID := op.WorkflowID + "-collect-" + op.CollectionID
	var runID string
	e = store.With(r.Context(), "hybrid-submission/"+op.WorkflowID, func(entry *durable.Entry) error {
		if e := entry.Read(&record); e != nil {
			return e
		}
		if record.ActiveCollection != "" && record.ActiveCollection != workflowID {
			description, e := s.temporalClient.DescribeWorkflowExecution(r.Context(), record.ActiveCollection, record.CollectionRunID)
			if e != nil {
				return e
			}
			if description.WorkflowExecutionInfo.Status == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
				return fmt.Errorf("another collection is active")
			}
		}
		if record.ActiveCollection == workflowID && record.CollectionRunID != "" {
			runID = record.CollectionRunID
			return nil
		}
		var req hybrid.Collection
		key := "hybrid-collection-request/" + workflowID
		if e := store.Read(r.Context(), key, &req); e != nil {
			if !os.IsNotExist(e) {
				return e
			}
			var prior hybrid.Result
			if e = store.Read(r.Context(), "hybrid-result/"+op.WorkflowID, &prior); e != nil {
				return fmt.Errorf("terminal computation evidence required: %w", e)
			}
			req = hybrid.Collection{Prepared: record.Prepared, Prior: prior, ID: op.CollectionID}
			if e = store.Write(r.Context(), key, req); e != nil {
				return e
			}
		}
		record.ActiveCollection = workflowID
		record.CollectionRunID = ""
		if e := entry.Write(record); e != nil {
			return e
		}
		run, e := s.temporalClient.ExecuteWorkflow(r.Context(), client.StartWorkflowOptions{ID: workflowID, TaskQueue: s.hybridConfig.Queue, WorkflowIDReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE}, hybrid.Collect, req)
		if e != nil {
			var already *serviceerror.WorkflowExecutionAlreadyStarted
			if !errors.As(e, &already) {
				return e
			}
			d, e := s.temporalClient.DescribeWorkflowExecution(r.Context(), workflowID, "")
			if e != nil {
				return e
			}
			runID = d.WorkflowExecutionInfo.Execution.RunId
		} else {
			runID = run.GetRunID()
		}
		record.CollectionRunID = runID
		return entry.Write(record)
	})
	if e != nil {
		writeError(w, 409, e.Error())
		return
	}
	json.NewEncoder(w).Encode(map[string]any{"state": "collecting", "workflow_id": op.WorkflowID, "collection_workflow_id": workflowID, "run_id": runID})
}
func (s *Server) handleHybridArtifacts(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, 405, "GET required")
		return
	}
	if !s.hybridEnabled(w) {
		return
	}
	id := hybridIdentity{Owner: r.URL.Query().Get("owner_id"), WorkflowID: r.URL.Query().Get("workflow_id")}
	_, e := s.ownedHybrid(r.Context(), id)
	if e != nil {
		writeError(w, 404, e.Error())
		return
	}
	var result hybrid.Result
	if e = (durable.Store{Root: s.hybridConfig.JournalRoot}).Read(r.Context(), "hybrid-result/"+id.WorkflowID, &result); e != nil {
		writeError(w, 409, "verified publication evidence is not yet available")
		return
	}
	if r.URL.Path == "/hybrid_artifacts" {
		json.NewEncoder(w).Encode(result)
		return
	}
	node, ok := result.Nodes[r.URL.Query().Get("node_id")]
	artifact, found := node.Artifacts[r.URL.Query().Get("artifact_id")]
	if !ok || !found {
		writeError(w, 404, "verified artifact not found")
		return
	}
	bucket, key, e := transfer.SplitS3(artifact.URI)
	if e != nil {
		writeError(w, 400, "artifact has no S3 locator")
		return
	}
	cfg, e := config.LoadDefaultConfig(r.Context(), config.WithRegion(s.hybridConfig.Region))
	if e != nil {
		writeError(w, 503, "artifact access configuration unavailable")
		return
	}
	input := &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)}
	if artifact.VersionID != "" {
		input.VersionId = aws.String(artifact.VersionID)
	}
	access, e := s3.NewPresignClient(s3.NewFromConfig(cfg)).PresignGetObject(r.Context(), input, func(o *s3.PresignOptions) { o.Expires = 5 * time.Minute })
	if e != nil {
		writeError(w, 503, "artifact access credentials unavailable")
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	json.NewEncoder(w).Encode(map[string]any{"artifact": artifact, "url": access.URL, "expires_seconds": 300, "method": access.Method, "checksum": fmt.Sprint(artifact.SHA256)})
}
