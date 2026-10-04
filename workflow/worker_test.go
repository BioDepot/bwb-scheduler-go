package workflow

import (
	"reflect"
	"runtime"
	"strings"
	"testing"
)

type recordingRegistry struct {
	names []string
}

func (r *recordingRegistry) RegisterActivity(activity interface{}) {
	name := runtime.FuncForPC(reflect.ValueOf(activity).Pointer()).Name()
	r.names = append(r.names, name[strings.LastIndex(name, ".")+1:])
}

// A node on a local worker whose input comes from another executor has that
// input transferred by an activity on the worker's own queue.
func TestCmdWorkerServesTransfers(t *testing.T) {
	registry := &recordingRegistry{}
	registerCmdWorkerActivities(registry)

	registered := make(map[string]bool)
	for _, name := range registry.names {
		registered[name] = true
	}
	for _, want := range []string{
		"WorkerHeartbeatActivity", "RunCmdActivity", "SetupRootDir",
		"TransferSshToLocalFS", "TransferLocalToLocalFS",
		"TransferLocalToSshFS", "TransferSshToSshFS",
	} {
		if !registered[want] {
			t.Errorf("activity %s is not registered on the command worker; got %v", want, registry.names)
		}
	}
}
