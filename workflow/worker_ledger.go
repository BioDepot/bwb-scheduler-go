package workflow

// Shared worker ledger.
//
// One WorkerLedgerWorkflow runs per (namespace, worker queue). It holds the
// only account of that worker's CPUs, memory and GPUs, so that every workflow
// in the namespace that names the queue is granted from the same capacity.
//
// Protocol (all by signal, to the ledger's workflow ID):
//
//	ledger-register    holder -> ledger   LedgerRegistration
//	ledger-registered  ledger -> holder   LedgerAck (Error set on refusal)
//	ledger-request     holder -> ledger   ResourceRequest (CallerRunId set)
//	allocation-response ledger -> holder  ResourceGrant (RecipientRunId set)
//	ledger-release     holder -> ledger   ResourceGrant
//	ledger-deregister  holder -> ledger   LedgerDeregistration
//
// Capacity rule: the first holder to register while the ledger has no holders
// sets the queue's capacity. A holder that registers a different capacity
// while any other holder is registered is refused with a clear error. When
// the last holder leaves, the next registration may set a new capacity.
//
// Reclaiming: a holder's grants and queued requests are freed when it
// deregisters (normal paths), when a grant signal cannot be delivered and a
// liveness check finds the holder closed, and by a periodic liveness check
// (an activity that describes each holder's execution) for abnormal ends:
// cancelled, terminated, timed out, or a client killed so that the workflow
// stops making progress and later fails.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"go-scheduler/parsing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
	ledgerRegisterSignal   = "ledger-register"
	ledgerRegisteredSignal = "ledger-registered"
	ledgerRequestSignal    = "ledger-request"
	ledgerReleaseSignal    = "ledger-release"
	ledgerDeregisterSignal = "ledger-deregister"
	// ledgerWorkerUpSignal is sent by a command worker when it starts polling
	// its queue, so that the ledger does not wait for its retry timer.
	ledgerWorkerUpSignal = "ledger-worker-up"

	ledgerTickSeconds     = 5
	ledgerLivenessSeconds = 30
	ledgerIdleSeconds     = 60
	ledgerMaxHistory      = 9000
	ledgerAckTimeout      = 2 * time.Minute
)

type LedgerHolder struct {
	WorkflowId string
	RunId      string
}

func (h LedgerHolder) key() string { return h.WorkflowId + "/" + h.RunId }

type LedgerRegistration struct {
	Queue    string
	Capacity parsing.ResourceVector
	Holder   LedgerHolder
}

type LedgerDeregistration struct {
	Holder LedgerHolder
}

type LedgerAck struct {
	Queue string
	Error string
}

type LedgerState struct {
	Queue string
	// Sched carries the worker's capacity and use, and the heartbeat
	// bookkeeping shared with the per-workflow scheduler.
	Sched   SchedWorkflowState
	Holders map[string]LedgerHolder
	Pending []ResourceRequest
	// Active is keyed by holder key and request ID.
	Active               map[string]ResourceGrant
	QuietSeconds         int
	SecondsSinceLiveness int
	IdleSeconds          int
}

func workerLedgerWorkflowID(queue string) string {
	sum := sha256.Sum256([]byte(queue))
	prefix := safeWorkflowIDComponent(queue)
	if len(prefix) > 32 {
		prefix = prefix[:32]
	}
	return fmt.Sprintf("worker-ledger-%s-%s", prefix, hex.EncodeToString(sum[:10]))
}

func activeKey(h LedgerHolder, requestID int) string {
	return fmt.Sprintf("%s#%d", h.key(), requestID)
}

func (st *LedgerState) init() {
	if st.Holders == nil {
		st.Holders = make(map[string]LedgerHolder)
	}
	if st.Active == nil {
		st.Active = make(map[string]ResourceGrant)
	}
	if st.Pending == nil {
		st.Pending = make([]ResourceRequest, 0)
	}
	if st.IdleSeconds == 0 {
		st.IdleSeconds = ledgerIdleSeconds
	}
	s := &st.Sched
	if s.PendingRequests == nil {
		s.PendingRequests = make([]ResourceRequest, 0)
	}
	if s.ActiveGrants == nil {
		s.ActiveGrants = make(map[int]bool)
	}
	if s.Workers == nil {
		s.Workers = make(map[string]WorkerInfo)
	}
	if _, ok := s.Workers[st.Queue]; !ok {
		s.Workers[st.Queue] = WorkerInfo{QueueId: st.Queue}
	}
	if s.NumRetries == nil {
		s.NumRetries = make(map[string]int)
	}
	if s.SecondsSinceHeartbeatSched == nil {
		s.SecondsSinceHeartbeatSched = make(map[string]int)
	}
	if s.SecondsUntilHeartbeatRetry == nil {
		s.SecondsUntilHeartbeatRetry = make(map[string]int)
	}
}

// register applies a registration and returns the acknowledgement.
func (st *LedgerState) register(reg LedgerRegistration) LedgerAck {
	ack := LedgerAck{Queue: st.Queue}
	if reg.Queue != st.Queue {
		ack.Error = fmt.Sprintf("ledger for queue %q received a registration for queue %q", st.Queue, reg.Queue)
		return ack
	}
	if _, ok := st.Holders[reg.Holder.key()]; ok {
		return ack
	}
	worker := st.Sched.Workers[st.Queue]
	if len(st.Holders) == 0 {
		worker.TotalResources = reg.Capacity
		worker.UsedResources = parsing.ResourceVector{}
		st.Sched.Workers[st.Queue] = worker
	} else if worker.TotalResources != reg.Capacity {
		ack.Error = fmt.Sprintf(
			"worker queue %q is shared with %d other running workflow(s) and is declared with "+
				"%d CPUs, %d MB memory and %d GPUs, but this workflow declares %d CPUs, %d MB memory "+
				"and %d GPUs; every workflow that names a queue must declare the same capacity "+
				"(use the same --cpus, --ram and --gpus) or wait until the other workflows have finished",
			st.Queue, len(st.Holders),
			worker.TotalResources.Cpus, worker.TotalResources.MemMb, worker.TotalResources.Gpus,
			reg.Capacity.Cpus, reg.Capacity.MemMb, reg.Capacity.Gpus,
		)
		return ack
	}
	st.Holders[reg.Holder.key()] = reg.Holder
	st.QuietSeconds = 0
	return ack
}

func (st *LedgerState) free(key string) {
	grant, ok := st.Active[key]
	if !ok {
		return
	}
	worker := st.Sched.Workers[st.Queue]
	worker.UsedResources.MemMb -= grant.Requirements.MemMb
	worker.UsedResources.Cpus -= grant.Requirements.Cpus
	worker.UsedResources.Gpus -= grant.Requirements.Gpus
	st.Sched.Workers[st.Queue] = worker
	delete(st.Active, key)
}

// reclaim removes a holder and everything it owns.
func (st *LedgerState) reclaim(h LedgerHolder) {
	kept := st.Pending[:0]
	for _, req := range st.Pending {
		if (LedgerHolder{req.CallerWorkflowId, req.CallerRunId}) != h {
			kept = append(kept, req)
		}
	}
	st.Pending = kept
	for key, grant := range st.Active {
		if (LedgerHolder{grant.RecipientWorkflowId, grant.RecipientRunId}) == h {
			st.free(key)
		}
	}
	delete(st.Holders, h.key())
}

func (st *LedgerState) request(req ResourceRequest) {
	h := LedgerHolder{req.CallerWorkflowId, req.CallerRunId}
	if _, ok := st.Holders[h.key()]; !ok {
		return
	}
	st.Pending = append(st.Pending, req)
	st.QuietSeconds = 0
}

// process grants pending requests with the per-workflow policy: highest rank,
// then GPUs, CPUs and memory, largest first; requests that tie keep arrival
// order, so workflows are served first-come first-served. A request that does
// not fit is skipped and later ones that fit are granted.
func (st *LedgerState) process() []ResourceGrant {
	sortStable(st.Pending)
	grants := make([]ResourceGrant, 0)
	worker := st.Sched.Workers[st.Queue]
	if !worker.IsAlive {
		return grants
	}
	for i := 0; i < len(st.Pending); {
		req := st.Pending[i]
		w := st.Sched.Workers[st.Queue]
		fits := w.TotalResources.MemMb-w.UsedResources.MemMb >= req.Requirements.MemMb &&
			w.TotalResources.Cpus-w.UsedResources.Cpus >= req.Requirements.Cpus &&
			w.TotalResources.Gpus-w.UsedResources.Gpus >= req.Requirements.Gpus
		if !fits {
			i++
			continue
		}
		w.UsedResources.MemMb += req.Requirements.MemMb
		w.UsedResources.Cpus += req.Requirements.Cpus
		w.UsedResources.Gpus += req.Requirements.Gpus
		st.Sched.Workers[st.Queue] = w
		grant := ResourceGrant{
			RequestId:           req.Id,
			WorkerId:            st.Queue,
			Requirements:        req.Requirements,
			Success:             true,
			RecipientWorkflowId: req.CallerWorkflowId,
			RecipientRunId:      req.CallerRunId,
		}
		st.Active[activeKey(LedgerHolder{req.CallerWorkflowId, req.CallerRunId}, req.Id)] = grant
		grants = append(grants, grant)
		st.Pending = append(st.Pending[:i], st.Pending[i+1:]...)
	}
	return grants
}

func sortStable(reqs []ResourceRequest) {
	// insertion sort keeps this stable without importing sort twice
	for i := 1; i < len(reqs); i++ {
		for j := i; j > 0 && requestLess(reqs[j], reqs[j-1]); j-- {
			reqs[j], reqs[j-1] = reqs[j-1], reqs[j]
		}
	}
}

func (st *LedgerState) idle() bool {
	return len(st.Holders) == 0 && len(st.Pending) == 0 && len(st.Active) == 0
}

// WorkerLedgerWorkflow is the account of one worker queue.
func WorkerLedgerWorkflow(ctx workflow.Context, state LedgerState) error {
	logger := workflow.GetLogger(ctx)
	state.init()
	queue := state.Queue
	state.Sched.BackoffV1 = workflow.GetVersion(ctx, "heartbeat-backoff-reset", workflow.DefaultVersion, 1) != workflow.DefaultVersion

	selector := workflow.NewSelector(ctx)
	worker := state.Sched.Workers[queue]
	worker.IsAlive = true
	state.Sched.Workers[queue] = worker
	cancelHeartbeat := registerHeartbeatActivity(ctx, selector, &state.Sched, queue)

	regCh := workflow.GetSignalChannel(ctx, ledgerRegisterSignal)
	reqCh := workflow.GetSignalChannel(ctx, ledgerRequestSignal)
	relCh := workflow.GetSignalChannel(ctx, ledgerReleaseSignal)
	deregCh := workflow.GetSignalChannel(ctx, ledgerDeregisterSignal)
	upCh := workflow.GetSignalChannel(ctx, ledgerWorkerUpSignal)

	onRegister := func(reg LedgerRegistration) {
		ack := state.register(reg)
		if ack.Error != "" {
			logger.Warn("Refused registration", "holder", reg.Holder, "error", ack.Error)
		}
		if err := workflow.SignalExternalWorkflow(
			ctx, reg.Holder.WorkflowId, reg.Holder.RunId, ledgerRegisteredSignal, ack,
		).Get(ctx, nil); err != nil {
			logger.Warn("Could not acknowledge registration", "holder", reg.Holder, "error", err)
		}
	}
	onRequest := func(req ResourceRequest) { state.request(req) }
	onRelease := func(g ResourceGrant) {
		state.free(activeKey(LedgerHolder{g.RecipientWorkflowId, g.RecipientRunId}, g.RequestId))
	}
	onDeregister := func(d LedgerDeregistration) {
		logger.Info("Holder deregistered", "holder", d.Holder, "activeGrants", len(state.Active), "pending", len(state.Pending))
		state.reclaim(d.Holder)
		logger.Info("After deregistration", "holders", len(state.Holders), "activeGrants", len(state.Active), "usedCpus", state.Sched.Workers[queue].UsedResources.Cpus)
	}

	onWorkerUp := func() {
		w := state.Sched.Workers[queue]
		if !state.Sched.BackoffV1 || w.IsAlive {
			return
		}
		// A poller exists on the queue now: mark the worker usable. If no
		// retried heartbeat is already in flight, start one now instead of
		// waiting for the retry timer. The failure count restarts when that
		// heartbeat has run longer than the schedule-to-start timeout, as for
		// a timer-driven retry.
		logger.Info("Worker is up, retrying heartbeat now", "workerId", queue)
		w.IsAlive = true
		state.Sched.Workers[queue] = w
		if _, inFlight := state.Sched.SecondsSinceHeartbeatSched[queue]; inFlight {
			return
		}
		delete(state.Sched.SecondsUntilHeartbeatRetry, queue)
		registerHeartbeatActivity(ctx, selector, &state.Sched, queue)
		state.Sched.SecondsSinceHeartbeatSched[queue] = 0
	}
	selector.AddReceive(upCh, func(c workflow.ReceiveChannel, _ bool) {
		var from string
		c.Receive(ctx, &from)
		onWorkerUp()
	})
	selector.AddReceive(regCh, func(c workflow.ReceiveChannel, _ bool) {
		var v LedgerRegistration
		c.Receive(ctx, &v)
		onRegister(v)
	})
	selector.AddReceive(reqCh, func(c workflow.ReceiveChannel, _ bool) {
		var v ResourceRequest
		c.Receive(ctx, &v)
		onRequest(v)
	})
	selector.AddReceive(relCh, func(c workflow.ReceiveChannel, _ bool) {
		var v ResourceGrant
		c.Receive(ctx, &v)
		onRelease(v)
	})
	selector.AddReceive(deregCh, func(c workflow.ReceiveChannel, _ bool) {
		var v LedgerDeregistration
		c.Receive(ctx, &v)
		onDeregister(v)
	})

	// drain applies any signals already buffered, so none is lost when the
	// workflow closes or continues as new.
	drain := func() {
		for {
			progressed := false
			var reg LedgerRegistration
			for regCh.ReceiveAsync(&reg) {
				onRegister(reg)
				progressed = true
			}
			var req ResourceRequest
			for reqCh.ReceiveAsync(&req) {
				onRequest(req)
				progressed = true
			}
			var rel ResourceGrant
			for relCh.ReceiveAsync(&rel) {
				onRelease(rel)
				progressed = true
			}
			var dereg LedgerDeregistration
			for deregCh.ReceiveAsync(&dereg) {
				onDeregister(dereg)
				progressed = true
			}
			var up string
			for upCh.ReceiveAsync(&up) {
				onWorkerUp()
				progressed = true
			}
			if !progressed {
				return
			}
		}
	}

	var tick func(workflow.Future)
	tick = func(workflow.Future) {
		for _, grant := range state.process() {
			logger.Debug("Allocating resource grant", "grant", grant)
			err := workflow.SignalExternalWorkflow(
				ctx, grant.RecipientWorkflowId, grant.RecipientRunId,
				"allocation-response", grant,
			).Get(ctx, nil)
			if err != nil {
				// Undeliverable: take the grant back and queue the request
				// again at the front. If the holder is gone, the liveness
				// check below reclaims it.
				logger.Warn("Failed to signal requesting workflow", "grant", grant, "error", err)
				state.free(activeKey(LedgerHolder{grant.RecipientWorkflowId, grant.RecipientRunId}, grant.RequestId))
				state.Pending = append([]ResourceRequest{{
					Id: grant.RequestId, Requirements: grant.Requirements,
					CallerWorkflowId: grant.RecipientWorkflowId, CallerRunId: grant.RecipientRunId,
				}}, state.Pending...)
				state.SecondsSinceLiveness = ledgerLivenessSeconds
			}
		}
		retryHeartbeats(ctx, selector, &state.Sched, ledgerTickSeconds)

		state.SecondsSinceLiveness += ledgerTickSeconds
		if state.SecondsSinceLiveness >= ledgerLivenessSeconds && len(state.Holders) > 0 {
			state.SecondsSinceLiveness = 0
			holders := make([]LedgerHolder, 0, len(state.Holders))
			for _, h := range state.Holders {
				holders = append(holders, h)
			}
			sortHolders(holders)
			var dead []LedgerHolder
			actCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
				TaskQueue:           SCHEDULER_QUEUE,
				StartToCloseTimeout: 30 * time.Second,
				RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 3},
			})
			if err := workflow.ExecuteActivity(actCtx, "CheckHoldersAlive", holders).Get(ctx, &dead); err != nil {
				logger.Warn("Holder liveness check failed", "error", err)
			}
			for _, h := range dead {
				logger.Info("Reclaiming resources of closed workflow", "holder", h)
				state.reclaim(h)
			}
		}

		if state.idle() {
			state.QuietSeconds += ledgerTickSeconds
		} else {
			state.QuietSeconds = 0
		}
		selector.AddFuture(workflow.NewTimer(ctx, ledgerTickSeconds*time.Second), tick)
	}
	selector.AddFuture(workflow.NewTimer(ctx, ledgerTickSeconds*time.Second), tick)

	for {
		selector.Select(ctx)
		if ctx.Err() != nil {
			cancelHeartbeat()
			return nil
		}
		if state.idle() && state.QuietSeconds >= state.IdleSeconds {
			drain()
			if state.idle() {
				cancelHeartbeat()
				return nil
			}
			state.QuietSeconds = 0
			continue
		}
		if workflow.GetInfo(ctx).GetCurrentHistoryLength() > ledgerMaxHistory {
			drain()
			cancelHeartbeat()
			return workflow.NewContinueAsNewError(ctx, WorkerLedgerWorkflow, state)
		}
	}
}

func sortHolders(hs []LedgerHolder) {
	for i := 1; i < len(hs); i++ {
		for j := i; j > 0 && hs[j].key() < hs[j-1].key(); j-- {
			hs[j], hs[j-1] = hs[j-1], hs[j]
		}
	}
}

// LedgerActivities needs the Temporal client of the worker that serves the
// scheduler queue.
type LedgerActivities struct {
	Client client.Client
}

// EnsureWorkerLedger starts the queue's ledger if it is not running and
// delivers the registration to it, as one atomic call. Workflow code cannot
// call SignalWithStart, so this activity does.
func (a *LedgerActivities) EnsureWorkerLedger(ctx context.Context, reg LedgerRegistration) error {
	id := workerLedgerWorkflowID(reg.Queue)
	_, err := a.Client.SignalWithStartWorkflow(
		ctx, id, ledgerRegisterSignal, reg,
		client.StartWorkflowOptions{ID: id, TaskQueue: SCHEDULER_QUEUE},
		WorkerLedgerWorkflow, LedgerState{Queue: reg.Queue},
	)
	return err
}

// CheckHoldersAlive returns the holders whose workflow execution is no longer
// running. Errors other than "not found" leave the holder in place.
func (a *LedgerActivities) CheckHoldersAlive(ctx context.Context, holders []LedgerHolder) ([]LedgerHolder, error) {
	dead := make([]LedgerHolder, 0)
	for _, h := range holders {
		resp, err := a.Client.DescribeWorkflowExecution(ctx, h.WorkflowId, h.RunId)
		if err != nil {
			var nf *serviceerror.NotFound
			if errors.As(err, &nf) {
				dead = append(dead, h)
			}
			continue
		}
		if resp.GetWorkflowExecutionInfo().GetStatus() != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
			dead = append(dead, h)
		}
	}
	return dead, nil
}
