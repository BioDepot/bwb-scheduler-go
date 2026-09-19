package staged

import (
	"encoding/json"
	"fmt"
	"os"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

func LoadConfig(paths ...string) (Config, error) {
	var config Config
	for _, path := range paths {
		if path == "" {
			continue
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return config, err
		}
		if err := json.Unmarshal(data, &config); err != nil {
			return config, fmt.Errorf("%s: %w", path, err)
		}
	}
	if !identifier.MatchString(config.Pipeline.TaskQueue) || (config.Pipeline.GlobusTaskQueue != "" && !identifier.MatchString(config.Pipeline.GlobusTaskQueue)) || len(config.Executors.Globus.AllowedEndpointIDs) == 0 {
		return config, fmt.Errorf("staged worker requires pipeline.task_queue and Globus endpoint allowlist")
	}
	if err := config.Executors.Slurm.Validate(); err != nil {
		return config, fmt.Errorf("slurm: %w", err)
	}
	if err := config.Executors.SSHDocker.Validate(); err != nil {
		return config, fmt.Errorf("ssh_docker: %w", err)
	}
	if len(config.Executors.SSHDocker.AllowedRoots) == 0 {
		return config, fmt.Errorf("ssh_docker.allowed_roots is required")
	}
	return config, nil
}

func StartWorkers(c client.Client, config Config) ([]worker.Worker, error) {
	activities := &Activities{Config: config}
	orchestrator := worker.New(c, config.Pipeline.TaskQueue, worker.Options{})
	orchestrator.RegisterWorkflow(Run)
	workers := []worker.Worker{orchestrator}
	type registration struct {
		Name string
		Fn   interface{}
	}
	registrations := map[string][]registration{}
	registrations[config.Executors.Slurm.Queue()] = append(registrations[config.Executors.Slurm.Queue()],
		registration{"staged.SubmitSlurm", activities.SubmitSlurm},
		registration{"staged.PollSlurm", activities.PollSlurm},
		registration{"staged.CancelSlurm", activities.CancelSlurm},
	)
	registrations[config.Executors.SSHDocker.Queue()] = append(registrations[config.Executors.SSHDocker.Queue()],
		registration{"staged.PreflightGPU", activities.PreflightGPU},
		registration{"staged.SetupGPU", activities.SetupGPU},
		registration{"staged.UploadGPU", activities.UploadGPU},
		registration{"staged.RunGPU", activities.RunGPU},
		registration{"staged.DownloadGPU", activities.DownloadGPU},
		registration{"staged.CleanupGPU", activities.CleanupGPU},
	)
	// A combined local worker can share the orchestration queue with Globus.
	globusQueue := config.Pipeline.GlobusTaskQueue
	if globusQueue == "" {
		globusQueue = config.Pipeline.TaskQueue
	}
	registrations[globusQueue] = append(registrations[globusQueue],
		registration{"staged.SubmitTransfer", activities.SubmitTransfer},
		registration{"staged.PollTransfer", activities.PollTransfer},
	)
	for queue, entries := range registrations {
		var w worker.Worker
		if queue == config.Pipeline.TaskQueue {
			w = orchestrator
		} else {
			w = worker.New(c, queue, worker.Options{MaxConcurrentActivityExecutionSize: 4})
			workers = append(workers, w)
		}
		for _, entry := range entries {
			w.RegisterActivityWithOptions(entry.Fn, activity.RegisterOptions{Name: entry.Name})
		}
	}
	for _, w := range workers {
		if err := w.Start(); err != nil {
			for _, started := range workers {
				if started == w {
					break
				}
				started.Stop()
			}
			return nil, err
		}
	}
	return workers, nil
}
