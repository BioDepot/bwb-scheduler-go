package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"go-scheduler/fs"
	"go-scheduler/parsing"
	"go-scheduler/server"
	"go-scheduler/workflow"
	"log"
	"log/slog"
	"math"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/shirou/gopsutil/mem"
	"github.com/spf13/cobra"
	"go.temporal.io/sdk/client"
	temporalLog "go.temporal.io/sdk/log"
)

type ByteSize int64

func temporalClientOptions(logger *slog.Logger) client.Options {
	hostPort := strings.TrimSpace(os.Getenv("BWB_TEMPORAL_ADDRESS"))
	if hostPort == "" {
		hostPort = "localhost:7233"
	}
	namespace := strings.TrimSpace(os.Getenv("BWB_TEMPORAL_NAMESPACE"))
	if namespace == "" {
		namespace = "default"
	}
	return client.Options{
		HostPort:  hostPort,
		Namespace: namespace,
		Logger:    temporalLog.NewStructuredLogger(logger),
	}
}

func (b *ByteSize) String() string {
	return fmt.Sprintf("%d", *b)
}

func (b *ByteSize) Type() string {
	return "bytes"
}

func (b *ByteSize) SetBytes(bytes int) {
	*b = ByteSize(bytes)
}

func (b *ByteSize) AsUnit(unit string) int {
	units := map[string]int{
		"KB":  1000,
		"MB":  1000 * 1000,
		"GB":  1000 * 1000 * 1000,
		"KIB": 1024,
		"MIB": 1024 * 1024,
		"GIB": 1024 * 1024 * 1024,
	}

	multiplier, ok := units[unit]
	if !ok {
		return -1
	}
	return int(*b) / multiplier
}

func (b *ByteSize) Set(s string) error {
	s = strings.TrimSpace(s)
	s = strings.ToUpper(s)

	units := map[string]int64{
		"KB":  1000,
		"MB":  1000 * 1000,
		"GB":  1000 * 1000 * 1000,
		"KIB": 1024,
		"MIB": 1024 * 1024,
		"GIB": 1024 * 1024 * 1024,
	}

	for unit, multiplier := range units {
		if strings.HasSuffix(s, unit) {
			numPart := strings.TrimSuffix(s, unit)
			val, err := strconv.ParseFloat(numPart, 64)
			if err != nil {
				return fmt.Errorf("invalid size %q: %w", s, err)
			}
			*b = ByteSize(val * float64(multiplier))
			return nil
		}
	}

	return fmt.Errorf("unknown size unit in %q", s)
}

func randomString(length int) string {
	b := make([]byte, length+2)
	rand.Read(b)
	return fmt.Sprintf("%x", b)[2 : length+2]
}

func overrideParamVals(
	bwbWorkflow parsing.Workflow, index *parsing.WorkflowIndex,
	paramVals []string,
) error {
	for _, paramValRaw := range paramVals {
		splitByEq := strings.Split(paramValRaw, "=")
		fmtBaseErr := fmt.Errorf(
			"could not parse param override %s; --param arguments must be "+
				" of the form [NODE_NAME].[PARAM_NAME]=[VAL], where NODE_NAME is "+
				"the title or numeric ID of a node defined in the workflow JSON, "+
				"PARAM_NAME is one of its parameters, and value is a JSON-parsable "+
				"string giving the desired value of this parameter", paramValRaw,
		)

		if len(splitByEq) != 2 {
			return fmtBaseErr
		}
		nodeParamIdStr := splitByEq[0]
		valStr, err := parsing.NormalizeLooseJSON(splitByEq[1])
		if err != nil {
			return fmtBaseErr
		}

		splitByDot := strings.Split(nodeParamIdStr, ".")
		if len(splitByDot) != 2 {
			return fmtBaseErr
		}
		nodeNameStr := splitByDot[0]
		pname := splitByDot[1]

		var v any
		dec := json.NewDecoder(strings.NewReader(valStr))
		dec.UseNumber()
		if err := dec.Decode(&v); err != nil {
			return fmt.Errorf(
				"%s %s is not a valid JSON value str", fmtBaseErr, valStr,
			)
		}

		nodeId, nodeNameIsTitle := index.GetIdFromTitle(nodeNameStr)
		invalidNodeNameErr := fmt.Errorf(
			"%s Node name %s is neither the title nor the numeric ID of a node",
			fmtBaseErr, nodeNameStr,
		)
		if !nodeNameIsTitle {
			var err error
			nodeId, err = strconv.Atoi(nodeNameStr)
			if err != nil {
				return invalidNodeNameErr
			}
		}

		argType, err := bwbWorkflow.GetArgType(nodeId, pname)
		if err != nil {
			return err
		}
		if err := index.AddParam(nodeId, pname, argType, v); err != nil {
			return err
		}
		if err := bwbWorkflow.SetParam(nodeId, pname, v); err != nil {
			return err
		}
	}
	return nil
}

func overrideConfigVals(
	jobConfig *parsing.JobConfig, useDocker bool,
) error {
	for name, config := range jobConfig.LocalConfigsByName {
		if useDocker {
			config.UseDocker = true
			jobConfig.LocalConfigsByName[name] = config
		}
	}
	for nodeId, config := range jobConfig.LocalConfigsByNode {
		if useDocker {
			config.UseDocker = true
			jobConfig.LocalConfigsByNode[nodeId] = config
		}
	}
	for name, config := range jobConfig.TemporalConfigsByName {
		if useDocker {
			config.UseDocker = true
			jobConfig.TemporalConfigsByName[name] = config
		}
	}
	for nodeId, config := range jobConfig.TemporalConfigsByNode {
		if useDocker {
			config.UseDocker = true
			jobConfig.TemporalConfigsByNode[nodeId] = config
		}
	}
	return nil
}

func convertOWSWorkflow(
	owsDirPath string, outPath string,
) error {
	workflow, err := parsing.ParseWorkflow(owsDirPath)
	if err != nil {
		return fmt.Errorf("error parsing OWS: %s", err)
	}

	jsonBytes, err := json.MarshalIndent(&workflow, "", "\t")
	if err != nil {
		return fmt.Errorf("error marshaling bulk rna seq workflow: %s", err)
	}

	if err = os.WriteFile(outPath, jsonBytes, 0644); err != nil {
		return fmt.Errorf("error writing to output file %s: %s", outPath, err)
	}

	return nil
}

func dryRunWorkflow(
	wfPath string, paramVals []string,
) error {
	bwbWorkflow, err := parseWorkflowFile(wfPath)
	if err != nil {
		return fmt.Errorf("workflow parsing error: %s", err)
	}

	index, err := parsing.ParseAndValidateWorkflow(bwbWorkflow)
	if err != nil {
		return fmt.Errorf("workflow validation error: %s", err)
	}

	if err := overrideParamVals(bwbWorkflow, &index, paramVals); err != nil {
		return err
	}

	// Lazy, make an ABC method to do this setup automatically later.
	if err := bwbWorkflow.PropagateArgTypes(); err != nil {
		return fmt.Errorf(
			"error propagating arg types in %s: %s",
			wfPath, err,
		)
	}

	out, err := bwbWorkflow.DryRun()
	if err != nil {
		return err
	}

	for _, cmd := range out {
		fmt.Println(cmd)
		fmt.Println()
	}
	return nil
}

func runWorkflowTemporal(
	bwbWorkflow parsing.Workflow, index parsing.WorkflowIndex,
	jobConfig parsing.JobConfig, masterFS fs.LocalFS,
	temporalWfName, storageId string, softFail bool,
	checkptPath string, logger *slog.Logger,
	localWorker workflow.WorkerInfo,
) error {
	var err error

	c, err := client.NewLazyClient(temporalClientOptions(logger))
	if err != nil {
		return fmt.Errorf("unable to create Temporal client: %s", err)
	}
	defer c.Close()

	cancelChan := make(chan any)
	queueName := localWorker.QueueId
	useTemporal := len(jobConfig.TemporalConfigsByNode) > 0
	if err := workflow.StartWorkers(c, jobConfig, queueName, cancelChan, useTemporal); err != nil {
		return err
	}

	wfName := temporalWfName
	if temporalWfName == "" {
		wfName = "bwb_workflow_" + time.Now().Format("20060102150405")
	}
	workflowOptions := client.StartWorkflowOptions{
		ID:        wfName,
		TaskQueue: "bwb_worker",
	}

	workers := map[string]workflow.WorkerInfo{queueName: localWorker}

	var we client.WorkflowRun
	if bwbWorkflow.GetVersion() == "biodepot.legacy" {
		// We can't pass an interface through temporal API, so we have to
		// make it a concrete type.
		v0Wf, ok := (bwbWorkflow).(*parsing.WorkflowV0)
		if !ok {
			return fmt.Errorf("workflow incorrectly asserts type as biodepot.legacy")
		}
		we, err = c.ExecuteWorkflow(
			context.Background(), workflowOptions,
			workflow.RunBwbWorkflowV0, storageId, jobConfig,
			*v0Wf, index, workers, masterFS, softFail,
		)
	} else if bwbWorkflow.GetVersion() == "biodepot.resolved_workflow/v1" {
		v1Wf, ok := (bwbWorkflow).(*parsing.ResolvedWorkflow)
		if !ok {
			return fmt.Errorf("workflow incorrectly asserts type as biodepot.resolved_workflow/v1")
		}
		we, err = c.ExecuteWorkflow(
			context.Background(), workflowOptions,
			workflow.RunBwbWorkflowV1, storageId, jobConfig,
			*v1Wf, index, workers, masterFS, softFail,
		)
	} else {
		return fmt.Errorf(
			"unimplemented workflow type %s", bwbWorkflow.GetVersion(),
		)
	}

	if err != nil {
		return fmt.Errorf("failed to start workflow: %s", err)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		signalCtx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()
		c.SignalWorkflow(signalCtx, we.GetID(), we.GetRunID(), "cancel", true)
	}()

	var resultErr error
	err = we.Get(context.Background(), &resultErr)
	cancelChan <- struct{}{}
	if err != nil {
		return fmt.Errorf("workflow failed with err: %s", err)
	} else {
		fmt.Println("Workflow completed successfully")
	}
	return nil
}

func runWorkflowNoTemporal(
	bwbWorkflow parsing.Workflow, index parsing.WorkflowIndex,
	jobConfig parsing.JobConfig, masterFS fs.LocalFS, storageId string,
	softFail bool, checkptPath string, logger *slog.Logger,
	localWorker workflow.WorkerInfo,
) error {
	ctx, cancelCtx := context.WithCancel(context.Background())
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		cancelCtx()
	}()

	return workflow.RunBwbWorkflowNoTemporal(
		ctx, storageId, jobConfig, bwbWorkflow, index,
		localWorker, masterFS, softFail, *logger,
	)
}

type ResolvedWorkflowEnvelope struct {
	Schema      string          `json:"schema"` // must equal "biodepot.resolved_workflow/v1"
	WorkflowRaw json.RawMessage `json:"resolved_workflow"`
}

func parseWorkflowFile(wfPath string) (parsing.Workflow, error) {
	data, err := os.ReadFile(wfPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read workflow file %s: %v", wfPath, err)
	}

	var rawWf ResolvedWorkflowEnvelope
	if err := json.Unmarshal(data, &rawWf); err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON file %s: %v", wfPath, err)
	}

	if rawWf.Schema == "biodepot.legacy" {
		var bwbWorkflow parsing.WorkflowV0
		if err := json.Unmarshal(rawWf.WorkflowRaw, &bwbWorkflow); err != nil {
			return nil, fmt.Errorf(
				"failed to unmarshal JSON file %s as legacy workflow: %v",
				wfPath, err,
			)
		}
		return &bwbWorkflow, nil
	} else if rawWf.Schema == "biodepot.resolved_workflow/v1" {
		var bwbWorkflow parsing.ResolvedWorkflow
		if err := json.Unmarshal(rawWf.WorkflowRaw, &bwbWorkflow); err != nil {
			return nil, fmt.Errorf(
				"failed to unmarshal JSON file %s as v1 workflow: %v",
				wfPath, err,
			)
		}
		return &bwbWorkflow, nil
	}

	return nil, fmt.Errorf("unknown schema type %s in file %s", rawWf.Schema, wfPath)
}

func runWorkflow(
	wfPath string, configPath *string, workerName, temporalWfName,
	storageId string, paramVals []string, noTemporal, softFail, useDocker,
	verbose bool, checkptPath string, workerRam ByteSize, workerCpus,
	workerGpus int,
) error {
	bwbWorkflow, err := parseWorkflowFile(wfPath)
	if err != nil {
		return err
	}

	index, err := parsing.ParseAndValidateWorkflow(bwbWorkflow)
	if err != nil {
		return fmt.Errorf("workflow validation error: %s", err)
	}

	if err := overrideParamVals(bwbWorkflow, &index, paramVals); err != nil {
		return err
	}

	if err := bwbWorkflow.PropagateArgTypes(); err != nil {
		return fmt.Errorf(
			"error propagating arg types in %s: %s",
			wfPath, err,
		)
	}

	if _, err := bwbWorkflow.DryRun(); err != nil {
		return fmt.Errorf("dry run failed with error: %s", err)
	}

	queueName := workerName
	if queueName == "" {
		queueName = randomString(16)
	}

	localWorker := workflow.WorkerInfo{
		QueueId: queueName,
		TotalResources: parsing.ResourceVector{
			MemMb: workerRam.AsUnit("MB"),
			Cpus:  workerCpus,
			Gpus:  workerGpus,
		},
	}

	sched_dir := os.Getenv("BWB_SCHED_DIR")
	revisedStorageID := "storageID"
	if storageId != "" {
		revisedStorageID = storageId
	}

	masterFS := fs.LocalFS{
		RootDir: filepath.Join(sched_dir, revisedStorageID),
	}

	slogLevel := slog.LevelInfo
	if verbose {
		slogLevel = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout,
		&slog.HandlerOptions{Level: slogLevel},
	))

	var jobConfig parsing.JobConfig
	if configPath != nil {
		jobConfig, err = parsing.ParseAndValidateJobConfigFile(*configPath)
		if err != nil {
			return fmt.Errorf(
				"error parsing job control file %s: %s",
				*configPath, err,
			)
		}
	} else {
		jobConfig = parsing.GetDefaultConfig(bwbWorkflow, noTemporal)
	}

	if err := overrideConfigVals(&jobConfig, useDocker); err != nil {
		return fmt.Errorf("error overriding job config params: %s", err)
	}

	if noTemporal {
		return runWorkflowNoTemporal(
			bwbWorkflow, index, jobConfig, masterFS, revisedStorageID,
			softFail, checkptPath, logger, localWorker,
		)
	}

	return runWorkflowTemporal(
		bwbWorkflow, index, jobConfig, masterFS, temporalWfName,
		revisedStorageID, softFail, checkptPath, logger, localWorker,
	)
}

func main() {
	rootCmd := &cobra.Command{
		Use:   "bwbScheduler",
		Short: "BWB Scheduler",
	}

	var noTemporal bool
	var useDocker bool
	var verbose bool
	var softFail bool
	var workerName string
	var storageId string
	var temporalWfName string
	var paramOverrides []string
	var checkptPath string
	var workerRam ByteSize
	var workerCpus int
	var workerGpus int
	runCmd := &cobra.Command{
		SilenceUsage: false,
		Use:          "run WORKFLOW_FILE [CONFIG_FILE]",
		Short:        "Run JSON-serialized BWB workflow",
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) < 1 || len(args) > 2 {
				return fmt.Errorf(
					"`run` takes mandatory workflow JSON as first arg and " +
						"optional config JSON as second arg, with no other args",
				)
			}
			return nil
		},
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 2 && noTemporal {
				return fmt.Errorf("cannot set config with --noTemporal")
			}
			if noTemporal && checkptPath != "" {
				return fmt.Errorf("cannot have --checkpt with --noTemporal")
			}
			if noTemporal && temporalWfName != "" {
				return fmt.Errorf("cannot have --temporalWfName with --noTemporal")
			}
			if noTemporal && workerName != "" {
				return fmt.Errorf("cannot have --workerName with --noTemporal")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			if !cmd.Flags().Changed("ram") {
				vmStat, err := mem.VirtualMemory()
				if err != nil {
					return fmt.Errorf("failed to get system RAM w/ error: %s", err)
				}

				workerRam.SetBytes(int(math.Floor(
					float64(vmStat.Total) * float64(0.7),
				)))
				fmt.Printf(
					"No worker RAM value set, defaulting to 70%% of system "+
						"total: %d MB\n", workerRam/1024/1024,
				)
			}

			if !cmd.Flags().Changed("cpus") {
				totalCpus := runtime.NumCPU()
				workerCpus = max(totalCpus/2, 1)
				fmt.Printf(
					"No worker CPUs value set, defaulting to 50%% of system "+
						"total: %d\n", workerCpus,
				)
			}

			var configPath *string = nil
			if len(args) > 1 {
				configPath = &args[1]
			}

			return runWorkflow(
				args[0], configPath, workerName, temporalWfName, storageId,
				paramOverrides, noTemporal, softFail, useDocker, verbose, checkptPath,
				workerRam, workerCpus, workerGpus,
			)
		},
	}
	runCmd.Flags().StringVar(
		&workerName, "workerName", "", "Temporal queue name of local worker. "+
			"Cannot be used in conjunction with --noTemporal.",
	)
	runCmd.Flags().StringVar(
		&temporalWfName, "temporalWfName", "", "Run ID for temporal workflow. "+
			"Cannot be used in conjunction with --noTemporal.",
	)
	runCmd.Flags().StringVar(
		&storageId, "storageId", "", "Storage ID of the workflow. "+
			"Workflows run with the same storage ID (and the same master FS) "+
			"will share filesystems.",
	)
	runCmd.Flags().BoolVar(
		&noTemporal, "noTemporal", false, "Run workflow without temporal",
	)
	runCmd.Flags().BoolVar(
		&useDocker, "docker", false, "Use docker for locally run containers "+
			"rather than singularity. Overrides values in job config.",
	)
	runCmd.Flags().BoolVarP(
		&verbose, "verbose", "v", false, "Output verbosity.",
	)
	runCmd.Flags().BoolVar(
		&softFail, "softFail", false,
		"Continue running workflow after encountering individual job failures,"+
			"until all cmds not dependant on the failed job have run.",
	)
	runCmd.Flags().StringArrayVarP(
		&paramOverrides, "param", "p", []string{},
		"String of form [NODE_NAME].[PARAM_NAME]=[VAL], where NODE_NAME is "+
			"the title or numeric ID of a node defined in the workflow JSON, "+
			"PARAM_NAME is one of its parameters, and value is a JSON-parsable "+
			"string giving the desired value of this parameter. This will "+
			"override the default parameters given in the JSON.",
	)
	runCmd.Flags().Var(
		&workerRam, "ram", "Max amount of RAM to use for the local worker "+
			"as a string formed like \"10GB\", \"500MB\", etc. Default is 70% of "+
			"system RAM.",
	)
	runCmd.Flags().IntVar(
		&workerCpus, "cpus", 0, "Max number of CPU cores to use on local worker.",
	)
	runCmd.Flags().IntVar(
		&workerGpus, "gpus", 0, "Max number of GPUs to use on local worker.",
	)

	var workersConfigPath string
	var workersRam ByteSize
	var workersCpus int
	var workersGpus int
	var workersQueueName string
	var workersVerbose bool

	workersCmd := &cobra.Command{
		Use: "workers",
		Short: "Start Temporal workers (always starts scheduler queue worker; " +
			"starts additional workers based on config)",
		Args: cobra.NoArgs,
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if !cmd.Flags().Changed("ram") {
				vmStat, err := mem.VirtualMemory()
				if err != nil {
					return fmt.Errorf("failed to get system RAM w/ error: %s", err)
				}
				workersRam.SetBytes(int(math.Floor(
					float64(vmStat.Total) * float64(0.7),
				)))
				fmt.Printf(
					"No worker RAM value set, defaulting to 70%% of system "+
						"total: %d MB\n", workersRam/1024/1024,
				)
			}
			if !cmd.Flags().Changed("cpus") {
				totalCpus := runtime.NumCPU()
				workersCpus = max(totalCpus/2, 1)
				fmt.Printf(
					"No worker CPUs value set, defaulting to 50%% of system "+
						"total: %d\n", workersCpus,
				)
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			slogLevel := slog.LevelInfo
			if workersVerbose {
				slogLevel = slog.LevelDebug
			}
			logger := slog.New(slog.NewTextHandler(os.Stdout,
				&slog.HandlerOptions{Level: slogLevel},
			))

			c, err := client.NewLazyClient(temporalClientOptions(logger))
			if err != nil {
				return fmt.Errorf("unable to create Temporal client: %s", err)
			}
			defer c.Close()

			queueName := workersQueueName
			if queueName == "" {
				queueName = randomString(16)
			}

			localWorker := workflow.WorkerInfo{
				QueueId: queueName,
				TotalResources: parsing.ResourceVector{
					MemMb: workersRam.AsUnit("MB"),
					Cpus:  workersCpus,
					Gpus:  workersGpus,
				},
			}
			_ = localWorker // available for future use / logging

			var jobConfig parsing.JobConfig
			if workersConfigPath != "" {
				jobConfig, err = parsing.ParseAndValidateJobConfigFile(workersConfigPath)
				if err != nil {
					return fmt.Errorf(
						"error parsing job config file %s: %s",
						workersConfigPath, err,
					)
				}
			} else {
				jobConfig = parsing.JobConfig{
					ExecTypeByNode:        map[int]parsing.ExecType{},
					SlurmConfigsByNode:    map[int]parsing.SlurmJobConfig{},
					SlurmConfigsByName:    map[string]parsing.SlurmJobConfig{},
					LocalConfigsByNode:    map[int]parsing.LocalJobConfig{},
					LocalConfigsByName:    map[string]parsing.LocalJobConfig{},
					TemporalConfigsByNode: map[int]parsing.LocalJobConfig{},
					TemporalConfigsByName: map[string]parsing.LocalJobConfig{},
				}
			}

			cancelChan := make(chan any)
			if err := workflow.StartWorkers(c, jobConfig, queueName, cancelChan, true); err != nil {
				return fmt.Errorf("error starting workers: %s", err)
			}

			logger.Info("workers started", "queue", queueName)

			sigChan := make(chan os.Signal, 1)
			signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
			<-sigChan

			logger.Info("shutting down workers")
			cancelChan <- struct{}{}

			return nil
		},
	}

	workersCmd.Flags().StringVar(
		&workersConfigPath, "config", "",
		"Path to job config JSON file. If omitted, only the scheduler queue and lcoal "+
			"workers are started. Needed for SLURM. E.g. use test_workflows/salmon_v1_config.json "+
			"for worker if you intend to set up necessary workers for that configuration of the WF.",
	)
	workersCmd.Flags().StringVar(
		&workersQueueName, "workerName", "",
		"Temporal queue name for the local worker. Defaults to a random string.",
	)
	workersCmd.Flags().Var(
		&workersRam, "ram",
		"Max amount of RAM for the local worker (e.g. \"10GB\", \"500MB\"). "+
			"Defaults to 70%% of system RAM.",
	)
	workersCmd.Flags().IntVar(
		&workersCpus, "cpus", 0,
		"Max number of CPU cores for the local worker. Defaults to 50%% of system total.",
	)
	workersCmd.Flags().IntVar(
		&workersGpus, "gpus", 0,
		"Max number of GPUs for the local worker.",
	)
	workersCmd.Flags().BoolVarP(
		&workersVerbose, "verbose", "v", false, "Output verbosity.",
	)

	dryRunCmd := &cobra.Command{
		Use:   "dryRun [WORKFLOW_FILE]",
		Short: "Print dry run of JSON-serialized BWB workflow",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return dryRunWorkflow(args[0], paramOverrides)
		},
	}
	dryRunCmd.Flags().StringArrayVarP(
		&paramOverrides, "param", "p", []string{},
		"String of form [NODE_NAME].[PARAM_NAME]=[VAL], where NODE_NAME is "+
			"the title or numeric ID of a node defined in the workflow JSON, "+
			"PARAM_NAME is one of its parameters, and value is a JSON-parsable "+
			"string giving the desired value of this parameter. This will "+
			"override the default parameters given in the JSON.",
	)
	var serveAddr string

	serveCmd := &cobra.Command{
		Use:   "serve",
		Short: "Start the HTTP scheduler API server",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			slogLevel := slog.LevelInfo
			if verbose {
				slogLevel = slog.LevelDebug
			}
			logger := slog.New(slog.NewTextHandler(os.Stdout,
				&slog.HandlerOptions{Level: slogLevel},
			))
			return server.ListenAndServe(serveAddr, logger)
		},
	}
	serveCmd.Flags().StringVar(
		&serveAddr, "addr", ":8080",
		"TCP address for the HTTP server to listen on (e.g. \":8080\").",
	)
	serveCmd.Flags().BoolVarP(
		&verbose, "verbose", "v", false, "Output verbosity.",
	)

	var outPath string
	convertOWSCmd := &cobra.Command{
		Use:   "convertOWS [OWS_DIR_PATH]",
		Short: "Convert workflow in BWB's OWS format to scheduler JSON format.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return convertOWSWorkflow(args[0], outPath)
		},
	}
	convertOWSCmd.Flags().StringVarP(
		&outPath, "output", "o", "",
		"File path where the converted workflow will be stored in JSON format.",
	)
	convertOWSCmd.MarkFlagRequired("output")

	rootCmd.AddCommand(runCmd)
	rootCmd.AddCommand(dryRunCmd)
	rootCmd.AddCommand(convertOWSCmd)
	rootCmd.AddCommand(workersCmd)
	rootCmd.AddCommand(serveCmd)
	if err := rootCmd.Execute(); err != nil {
		log.Fatalln(err)
	}
}
