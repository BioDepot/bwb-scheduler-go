package workflow

import (
	"context"
	"errors"
	"go-scheduler/parsing"
	"log/slog"
	"math"
	"sort"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

type ResourceRequest struct {
    Id               int
    Rank             int
    Requirements     parsing.ResourceVector
    CallerWorkflowId string
    // CallerRunId is set for requests sent to a worker ledger.
    CallerRunId      string
}

type ResourceGrant struct {
    RequestId               int
    WorkerId                string
    Requirements            parsing.ResourceVector
    Success                 bool
    RecipientWorkflowId     string
    // RecipientRunId is set for grants issued by a worker ledger.
    RecipientRunId          string
}

type WorkerInfo struct {
    QueueId        string
    IsAlive        bool
    TotalResources parsing.ResourceVector
    UsedResources  parsing.ResourceVector
}

type SchedWorkflowState struct {
    PendingRequests            []ResourceRequest
    ActiveGrants               map[int]bool
    Workers                    map[string]WorkerInfo
    NumRetries                 map[string]int
    SecondsSinceHeartbeatSched map[string]int
    SecondsUntilHeartbeatRetry map[string]int
    // BackoffV1 selects the bounded back-off that resets after a success. It
    // is set from workflow.GetVersion at the start of each run.
    BackoffV1 bool
}

const (
    SCHEDULE_TO_START_TIMEOUT_SECONDS = 30
    HEARTBEAT_RETRY_MAX_SECONDS       = 9000
    HEARTBEAT_RETRY_BASE_SECONDS      = 30
    HEARTBEAT_RETRY_BACKOFF_COEFF     = 4

    // Version 1: wait 30 s after the first consecutive failure, doubling each
    // time up to 300 s; the count restarts after a successful heartbeat.
    HEARTBEAT_RETRY_V1_BASE_SECONDS = 30
    HEARTBEAT_RETRY_V1_MAX_SECONDS  = 300
)

func WorkerHeartbeatActivity(ctx context.Context, workerId string) error {
    for {
        select {
        case <-time.After(10 * time.Second):
            activity.RecordHeartbeat(ctx, workerId)
        case <-ctx.Done():
            return ctx.Err()
        }
    }
}

// markWorkerDead records a failed heartbeat: the worker is not used until the
// next retry, which waits longer after each consecutive failure.
func markWorkerDead(state *SchedWorkflowState, queueId string) {
    workerRecord := state.Workers[queueId]
    workerRecord.IsAlive = false
    state.Workers[queueId] = workerRecord

    if _, ok := state.NumRetries[queueId]; !ok {
        state.NumRetries[queueId] = 1
    } else {
        state.NumRetries[queueId] += 1
    }

    if state.BackoffV1 {
        state.SecondsUntilHeartbeatRetry[queueId] = heartbeatRetryWaitV1(state.NumRetries[queueId])
    } else {
        // Shockingly, go has no built-in integer exponentiation.
        multiple := int(math.Pow(
            HEARTBEAT_RETRY_BACKOFF_COEFF,
            float64(state.NumRetries[queueId]-1),
        ))

        state.SecondsUntilHeartbeatRetry[queueId] = min(
            HEARTBEAT_RETRY_BASE_SECONDS*multiple,
            HEARTBEAT_RETRY_MAX_SECONDS,
        )
    }
    delete(state.SecondsSinceHeartbeatSched, queueId)
}

func registerHeartbeatActivity(
    ctx workflow.Context, selector workflow.Selector,
    state *SchedWorkflowState, queueId string,
) func() {
    logger := workflow.GetLogger(ctx)
    ao := workflow.ActivityOptions{
        TaskQueue:              queueId,
        HeartbeatTimeout:       time.Minute,
        StartToCloseTimeout:    time.Hour * 24,
        ScheduleToStartTimeout: time.Second * SCHEDULE_TO_START_TIMEOUT_SECONDS,
        RetryPolicy: &temporal.RetryPolicy{
            MaximumAttempts: 1,
        },
    }
    heartbeatCtx, cancelFunc := workflow.WithCancel(
        workflow.WithActivityOptions(ctx, ao),
    )
    logger.Info("Registering heartbeat", "workerId", queueId)
    heartbeatFuture := workflow.ExecuteActivity(heartbeatCtx, WorkerHeartbeatActivity, queueId)
    selector.AddFuture(heartbeatFuture, func(f workflow.Future) {
        err := f.Get(ctx, nil)
        if err != nil {
            logger.Error("Worker heartbeat error", "workerId", queueId, "error", err)
            var timeoutErr *temporal.TimeoutError
            if errors.As(err, &timeoutErr) {
                markWorkerDead(state, queueId)
            }
        }
    })
    return cancelFunc
}

// heartbeatRetryWaitV1 is the wait before the next heartbeat attempt after
// the n-th consecutive failure: 30, 60, 120, 240, then 300 seconds.
func heartbeatRetryWaitV1(failures int) int {
    wait := HEARTBEAT_RETRY_V1_BASE_SECONDS
    for i := 1; i < failures && wait < HEARTBEAT_RETRY_V1_MAX_SECONDS; i++ {
        wait *= 2
    }
    return min(wait, HEARTBEAT_RETRY_V1_MAX_SECONDS)
}

// resurrectWorkers marks workers usable again once their retried heartbeat has
// outlived the schedule-to-start timeout, which means a poller took it. In
// version 1 that also ends the run of consecutive failures.
func resurrectWorkers(state *SchedWorkflowState, secondsElapsed int) {
    resurrectedWorkers := make([]string, 0)
    for queueId := range state.SecondsSinceHeartbeatSched {
        state.SecondsSinceHeartbeatSched[queueId] += secondsElapsed
        if state.SecondsSinceHeartbeatSched[queueId] > SCHEDULE_TO_START_TIMEOUT_SECONDS {
            workerRecord := state.Workers[queueId]
            workerRecord.IsAlive = true
            state.Workers[queueId] = workerRecord
            resurrectedWorkers = append(resurrectedWorkers, queueId)
            if state.BackoffV1 {
                delete(state.NumRetries, queueId)
            }
        }
    }

    for _, queueId := range resurrectedWorkers {
        delete(state.SecondsSinceHeartbeatSched, queueId)
    }
}

func retryHeartbeats(
    ctx workflow.Context, selector workflow.Selector,
    state *SchedWorkflowState, secondsElapsed int,
) {
    logger := workflow.GetLogger(ctx)
    resurrectWorkers(state, secondsElapsed)

    retriedWorkers := make([]string, 0)
    for queueId := range state.SecondsUntilHeartbeatRetry {
        state.SecondsUntilHeartbeatRetry[queueId] -= secondsElapsed
        if state.SecondsUntilHeartbeatRetry[queueId] <= 0 {
            logger.Info(
                "Retrying heartbeat", "workerId", queueId, "secondsSinceLastRetry", 
                state.SecondsUntilHeartbeatRetry[queueId],
            )
            registerHeartbeatActivity(ctx, selector, state, queueId)
            state.SecondsSinceHeartbeatSched[queueId] = 0
            retriedWorkers = append(retriedWorkers, queueId)
        }
    }

    for _, queueId := range retriedWorkers {
        delete(state.SecondsUntilHeartbeatRetry, queueId)
    }
}

// requestLess is the scheduling order: rank, then GPUs, CPUs and memory, each
// largest first.
func requestLess(reqI, reqJ ResourceRequest) bool {
    if reqI.Rank != reqJ.Rank {
        return reqI.Rank > reqJ.Rank
    }
    if reqI.Requirements.Gpus != reqJ.Requirements.Gpus {
        return reqI.Requirements.Gpus > reqJ.Requirements.Gpus
    }
    if reqI.Requirements.Cpus != reqJ.Requirements.Cpus {
        return reqI.Requirements.Cpus > reqJ.Requirements.Cpus
    }
    return reqI.Requirements.MemMb > reqJ.Requirements.MemMb
}

func processRequests(state *SchedWorkflowState) []ResourceGrant {
    sort.Slice(state.PendingRequests, func(i, j int) bool {
        return requestLess(state.PendingRequests[i], state.PendingRequests[j])
    })

    responses := make([]ResourceGrant, 0)
    for i := 0; i < len(state.PendingRequests); {
        req := state.PendingRequests[i]
        assigned := false

        for queueId, worker := range state.Workers {
            if !worker.IsAlive {
                continue
            }

            available := parsing.ResourceVector{
                MemMb: worker.TotalResources.MemMb - worker.UsedResources.MemMb,
                Cpus:  worker.TotalResources.Cpus - worker.UsedResources.Cpus,
                Gpus:  worker.TotalResources.Gpus - worker.UsedResources.Gpus,
            }

            if available.MemMb >= req.Requirements.MemMb &&
                available.Cpus >= req.Requirements.Cpus &&
                available.Gpus >= req.Requirements.Gpus {

                worker.UsedResources.MemMb += req.Requirements.MemMb
                worker.UsedResources.Cpus += req.Requirements.Cpus
                worker.UsedResources.Gpus += req.Requirements.Gpus
                state.Workers[queueId] = worker

                response := ResourceGrant{
                    RequestId: req.Id, 
                    WorkerId: queueId,
                    Requirements: req.Requirements,
                    Success:  true,
                    RecipientWorkflowId: req.CallerWorkflowId,
                }
                state.ActiveGrants[req.Id] = true

                responses = append(responses, response)

                state.PendingRequests = append(state.PendingRequests[:i], state.PendingRequests[i+1:]...)
                assigned = true
                break
            }
        }

        if !assigned {
            i++
        }
    }
    return responses
}

func freeResourceGrant(state *SchedWorkflowState, grant ResourceGrant) {
    // Temporal can occasionally reschedule the same signal / activity,
    // so we add this check to make the release signal handler idempotent.
    if !state.ActiveGrants[grant.RequestId] {
        return
    }

    worker := state.Workers[grant.WorkerId]
    worker.UsedResources.MemMb -= grant.Requirements.MemMb
    worker.UsedResources.Cpus -= grant.Requirements.Cpus
    worker.UsedResources.Gpus -= grant.Requirements.Gpus
    state.Workers[grant.WorkerId] = worker
    state.ActiveGrants[grant.RequestId] = false
}

func LocalResourceScheduler(
    logger slog.Logger, workers map[string]WorkerInfo, 
    reqChan <-chan ResourceRequest, releaseChan <-chan ResourceGrant,
    respChan chan<- ResourceGrant, doneChan <-chan bool,
) {
    state := SchedWorkflowState{
        Workers: workers,
        PendingRequests: make([]ResourceRequest, 0),
        ActiveGrants: make(map[int]bool),
    }

    for id := range state.Workers {
        worker := state.Workers[id]
        worker.IsAlive = true
        state.Workers[id] = worker
    }

    workflowComplete := false
    ticker := time.NewTicker(5 * time.Second)

    for !workflowComplete {
        select {
        case req := <- reqChan: {
            logger.Debug("Received new resource request", "request", req)
            state.PendingRequests = append(state.PendingRequests, req)
        }
        case grantToRelease := <- releaseChan: {
            logger.Debug("Freeing resource grant", "grant", grantToRelease)
            freeResourceGrant(&state, grantToRelease)
        }
        case <- ticker.C: {
            grants := processRequests(&state)
            for _, grant := range grants {
                logger.Debug("Allocating resource grant", "grant", grant)
                respChan <- grant
            }
        }
        case <- doneChan: {
            workflowComplete = true
        }
        }
    }
}

func ResourceSchedulerWorkflow(ctx workflow.Context, state SchedWorkflowState) error {
    logger := workflow.GetLogger(ctx)
    logger.Debug("(Re-) Starting sched workflow", "state", state)
    state.BackoffV1 = workflow.GetVersion(ctx, "heartbeat-backoff-reset", workflow.DefaultVersion, 1) != workflow.DefaultVersion
    if state.PendingRequests == nil {
        state.PendingRequests = make([]ResourceRequest, 0)
    }
    if state.Workers == nil {
        state.Workers = make(map[string]WorkerInfo)
    }
    if state.SecondsSinceHeartbeatSched == nil {
        state.SecondsSinceHeartbeatSched = make(map[string]int)
    }
    if state.SecondsUntilHeartbeatRetry == nil {
        state.SecondsUntilHeartbeatRetry = make(map[string]int)
    }
    if state.NumRetries == nil {
        state.NumRetries = make(map[string]int)
    }
    if state.ActiveGrants == nil {
        state.ActiveGrants = make(map[int]bool)
    }

    selector := workflow.NewSelector(ctx)
    heartbeatCancelFuncs := make([]func(), 0)
    for queueId := range state.Workers {
        heartbeatFuture := registerHeartbeatActivity(
            ctx, selector, &state, queueId,
        )
        heartbeatCancelFuncs = append(heartbeatCancelFuncs, heartbeatFuture)
        workerRecord := state.Workers[queueId]
        workerRecord.IsAlive = true
        state.Workers[queueId] = workerRecord
    }

    selector.AddReceive(workflow.GetSignalChannel(ctx, "new-request"), func(c workflow.ReceiveChannel, _ bool) {
        var req ResourceRequest
        c.Receive(ctx, &req)
        state.PendingRequests = append(state.PendingRequests, req)
        logger.Debug("Received new resource request", "request", req)
    })

    selector.AddReceive(workflow.GetSignalChannel(ctx, "release-allocation"), func(c workflow.ReceiveChannel, _ bool) {
        var grant ResourceGrant
        c.Receive(ctx, &grant)
        freeResourceGrant(&state, grant)
        logger.Debug("Freeing resource grant", "grant", grant)
    })

    selector.AddReceive(workflow.GetSignalChannel(ctx, "worker-register"), func(c workflow.ReceiveChannel, _ bool) {
        var worker WorkerInfo
        c.Receive(ctx, &worker)
        if _, exists := state.Workers[worker.QueueId]; !exists {
            worker.IsAlive = true
            state.Workers[worker.QueueId] = worker
            heartbeatFuture := registerHeartbeatActivity(
                ctx, selector, &state, worker.QueueId,
            )
            heartbeatCancelFuncs = append(heartbeatCancelFuncs, heartbeatFuture)
            logger.Info("Registered new worker", "workerId", worker.QueueId)
        }
    })

    durationSecs := 5
    durationSecsAsTime := time.Second * 5
    var timerCallback func(workflow.Future)
    timerCallback = func(f workflow.Future) {
        responses := processRequests(&state)
        for _, response := range responses {
            logger.Debug("Allocating resource grant", "grant", response)

            err := workflow.SignalExternalWorkflow(
                ctx, response.RecipientWorkflowId, "", 
                "allocation-response", response,
            ).Get(ctx, nil)
            if err != nil {
                logger.Error("Failed to signal requesting workflow", "error", err)
            }
        }
        retryHeartbeats(ctx, selector, &state, durationSecs)
        timer := workflow.NewTimer(ctx, durationSecsAsTime)
        selector.AddFuture(timer, timerCallback)
    }

    timer := workflow.NewTimer(ctx, durationSecsAsTime)
    selector.AddFuture(timer, timerCallback)

    cancelled := false
    for {
        selector.Select(ctx)
        if ctx.Err() != nil{
            cancelled = true
            break
        }

        if workflow.GetInfo(ctx).GetCurrentHistoryLength() > 9000 {
            logger.Debug(
                "Sched workflow history approaching limit, continuing as new",
            )
            break
        }
    }

    for _, cancelFunc := range heartbeatCancelFuncs {
        cancelFunc()
    }

    if cancelled {
        return nil
    }

    return workflow.NewContinueAsNewError(ctx, ResourceSchedulerWorkflow, state)
}
