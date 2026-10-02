package workflow

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
)

// The submission activity runs with a heartbeat timeout of a few seconds. On a
// site where its SSH and rsync calls take longer than that, it must report
// that it is alive while they run, from its first moment.
func TestHeartbeatWhileReportsDuringSlowWork(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()
	var beats atomic.Int32
	var lastDetail atomic.Value
	env.SetOnActivityHeartbeatListener(func(_ *activity.Info, details converter.EncodedValues) {
		beats.Add(1)
		var key string
		if err := details.Get(&key); err == nil {
			lastDetail.Store(key)
		}
	})
	var beatsBeforeWorkEnded int32
	slowWork := func(ctx context.Context) error {
		stop := heartbeatWhile(ctx, 5*time.Millisecond, "morphic-test")
		// Stands in for the remote calls: nothing in here heartbeats.
		time.Sleep(100 * time.Millisecond)
		beatsBeforeWorkEnded = beats.Load()
		stop()
		return nil
	}
	env.RegisterActivity(slowWork)
	if _, err := env.ExecuteActivity(slowWork); err != nil {
		t.Fatalf("activity failed: %v", err)
	}
	if beatsBeforeWorkEnded < 1 {
		t.Fatalf("no heartbeat was recorded while the work ran")
	}
	if got, _ := lastDetail.Load().(string); got != "morphic-test" {
		t.Fatalf("heartbeat detail = %q, want the correlation key", got)
	}
}

func TestHeartbeatWhileStopsWhenToldTo(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()
	done := make(chan struct{})
	work := func(ctx context.Context) error {
		stop := heartbeatWhile(ctx, time.Millisecond, "morphic-test")
		stop() // returns only after the heartbeat goroutine has ended
		close(done)
		return nil
	}
	env.RegisterActivity(work)
	if _, err := env.ExecuteActivity(work); err != nil {
		t.Fatalf("activity failed: %v", err)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("stop did not return")
	}
}
