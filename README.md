# Biodepot Scheduler

## Repository Lineage

`BioDepot/bwb-scheduler-go` is the BioDepot-maintained home of the Go
scheduler. The `agentCompat` branch includes Patrick McKeever's upstream
`agentCompat` history through `f948d22` plus reviewed BioDepot integration
fixes. The `patrick/agentCompat` branch preserves the unmodified upstream head,
and `https://github.com/Patrick-McKeever/bwb_scheduler.git` is tracked as the
read-only `upstream` remote in maintained checkouts.

## Description
This scheduler is based on Temporal.io workflow framework with key modifications to optimize bioinformatics workflows. We support a hybrid architecture across cloud, HPC, and local servers. Asynchronous execution is also supported to reduce execution time. This workflow has been tested using bulk RNA-seq datasets.


## Installation

### Dependencies
- Singularity > 3.5.0
- Docker (if running locally)
- Singularity / Docker nvidia toolkits (if using GPU-based workflows)
- Rsync (if using HPC)

**TODO**: Make an install script. For now, you can just compile the code, but I'll get to this shortly.

## CLI

**Note**: All CLI commands expect `BWB_SCHED_DIR` to be set as an environment variable; this will be mounted to `/data/` locally. (If using a storage ID to isolate workflow FSs, then `BWB_SCHED_DIR/[storageID]` will be mounted instead). Files should be staged here before running.

### OWS to JSON conversion
```
Usage:
  bwbScheduler convertOWS [OWS_DIR_PATH] [flags]

Flags:
  -h, --help            help for convertOWS
  -o, --output string   File path where the converted workflow will be stored in JSON format.
```

### Dry Run
```
Usage:
  bwbScheduler dryRun [WORKFLOW_FILE] [flags]

Flags:
  -h, --help                help for dryRun
  -p, --param stringArray   String of form [NODE_NAME].[PARAM_NAME]=[VAL], where NODE_NAME is the title or numeric ID of a node defined in the workflow JSON, PARAM_NAME is one of its parameters, and value is a JSON-parsable string giving the desired value of this parameter. This will override the default parameters given in the JSON.
```

### Running a workflow
```
Usage:
  bwbScheduler run WORKFLOW_FILE [CONFIG_FILE] [flags]

Flags:
      --cpus int                Max number of CPU cores to use on local worker.
      --docker                  Use docker for locally run containers rather than singularity. Overrides values in job config.
      --gpus int                Max number of GPUs to use on local worker.
  -h, --help                    help for run
      --noTemporal              Run workflow without temporal
  -p, --param stringArray       String of form [NODE_NAME].[PARAM_NAME]=[VAL], where NODE_NAME is the title or numeric ID of a node defined in the workflow JSON, PARAM_NAME is one of its parameters, and value is a JSON-parsable string giving the desired value of this parameter. This will override the default parameters given in the JSON.
      --ram bytes               Max amount of RAM to use for the local worker as a string formed like "10GB", "500MB", etc. Default is 70% of system RAM.
      --softFail                Continue running workflow after encountering individual job failures,until all cmds not dependant on the failed job have run.
      --storageId string        Storage ID of the workflow. Workflows run with the same storage ID (and the same master FS) will share filesystems.
      --temporalWfName string   Run ID for temporal workflow. Cannot be used in conjunction with --noTemporal.
      --workerName string       Temporal queue name of local worker. Cannot be used in conjunction with --noTemporal.
```

### Running the workflow server
```
Start the HTTP scheduler API server

Usage:
  bwbScheduler serve [flags]

Flags:
      --addr string   TCP address for the HTTP server to listen on (e.g. ":8080"). (default ":8080")
  -h, --help          help for serve
  -v, --verbose       Output verbosity.
```

### Running a worker independently
```
Start Temporal workers (always starts scheduler queue worker; starts additional workers based on config)

Usage:
  bwbScheduler workers [flags]

Flags:
      --config string       Path to job config JSON file. If omitted, only the scheduler queue and lcoal workers are started. Needed for SLURM. E.g. use test_workflows/salmon_v1_config.json for worker if you intend to set up necessary workers for that configuration of the WF.
      --cpus int            Max number of CPU cores for the local worker. Defaults to 50%% of system total.
      --gpus int            Max number of GPUs for the local worker.
  -h, --help                help for workers
      --ram bytes           Max amount of RAM for the local worker (e.g. "10GB", "500MB"). Defaults to 70%% of system RAM.
  -v, --verbose             Output verbosity.
      --workerName string   Temporal queue name for the local worker. Defaults to a random string.
```


## Example usage
 ### Bulk RNA workflow
 **Main repo**: https://github.com/morphic-bio/Bulk-RNA-seq/
```
# Set BWB_SCHED_DIR and stage dependencies there.
export BWB_SCHED_DIR=/home/user/SCHED_STORAGE
mkdir -p $BWB_SCHED_DIR/storageID/fastqFiles
mv R1.fastq R2.fastq $BWB_SCHED_DIR/storageID/fastqFiles


# Run bulk RNA seq workflow on R1/R2.fastq using hybrid HPC config.
go run main.go run --storageId storageID -p Start.fastqDir=/data/fastqFiles test_workflows/bulkrna_async.json test_workflows/bulkrna_config_hybrid.json
```


### Single-cell RNA workflow
**Main repo**: https://github.com/morphic-bio/scRNA-seq
```
go run main.go run --storageId scRNA --cpus 24 -v -p 7.whitelist=/data/10x_version3_whitelist.txt -p 7.features_file=/data/project_14361_feature_ref_v2_202405_alt.csv -p 7.fastqdirs=[/data/experimentA,/data/experimentB] -p 7.feature_barcode_fastq_dirs=[/data/larry-barcodes/experimentA,/data/larry-barcodes/experimentB] -p 7.feature_expr_fastq_dirs=[/data/experimentA,/data/experimentB] test_workflows/scRNA.json
```

**TODO**: Document this more thoroughly.

 ### Nanopore fusion detection workflow

 **Main repo**: https://github.com/BioDepot/fast-bff
```
export BWB_SCHED_DIR=/home/user/SCHED_STORAGE
mkdir -p $BWB_SCHED_DIR/storageID/fastqFiles
mv hg38.idx $BWB_SCHED_DIR/storageID/hg38.idx
mv 1.pod5 2.pod5 $BWB_SCHED_DIR/storageID/pod5
echo "chr15:74318559:74336132;chr17:38428464:38512385" > /data/bps

go run main.go run --storageId storageID -p 9.minimapIdx=/data/hg38.idx -p 9.pod5InDir=/data/pod5 -p 9.outputDir=/data/output -p 9.bpsFile=/data/bps test_workflows/fusion_finder_nanopore.json
```

`9.bpsFile` is a file in which each line specifies one candidate fusion as a pair of genomic intervals separated by a semicolon:

- One fusion per line.
- Each side is written as: chr:start:end
- You can omit any of chr, start, or end if unknown.
- Lines starting with “#” are treated as comments.

E.g.
```
# Fully specified intervals
chr15:74318559:74336132;chr17:38428464:38512385

# Open-ended intervals (missing start or end)
chr15:74318559:;chr17:38428464:38512385
chr15::74336132;chr17::

# Only the left side is provided (the right side is unconstrained)
chr15::74336132;
```

## API

See `serverIntegrationTest.py` for a full example of setting up workers, starting the server, and starting and polling a workflow. This script assumes that temporal server is running at localhost:7233 and that `/test/data/Arabidopsis_thaliana.TAIR10.28.cdna.all.fa.gz` and `/work/in/DRR016134_1.fastq.gz` exist locally, as required by the `test_workflows/salmon_v1.json` workflow.

| Method | Path | Request Body | Response (200) |
|--------|------|-------------|----------------|
| `POST` | `/start_workflow` | `{"schema": "biodepot.resolved_workflow/v1", "resolved_workflow": <ResolvedWorkflow>, "worker_info": {"QueueId": string, "IsAlive": bool, "TotalResources": {"MemMb": int, "Cpus": int, "Gpus": int}, "UsedResources": {"MemMb": int, "Cpus": int, "Gpus": int}}, "config": <JobConfig> \| null}` | `{"workflow_id": string, "run_id": string}` |
| `POST` | `/stop_workflow` | `{"workflow_id": string, "run_id": string \| null}` | `{"message": string}` |
| `POST` | `/workflow_status` | `{"workflow_id": string, "run_id": string \| null}` | `{"workflow_status": "RUNNING\|FINISHED\|FAILED\|CANCELED\|TERMINATED\|TIMED_OUT", "node_statuses": {<node_id>: "RUNNING\|FINISHED\|FAILED\|CANCELED\|TERMINATED\|TIMED_OUT"} \| null}` |

### Notes

- `config` in `/start_workflow` is optional; if omitted, a default config is generated from the workflow. When present, it uses the same `executors` / `configs` / `node_configs` JSON format accepted by the CLI config file.
- `run_id` in `/stop_workflow` and `/workflow_status` is optional; if omitted, the latest run for the given `workflow_id` is used.
- `node_statuses` in `/workflow_status` is `null` when the workflow has reached a terminal status, since completed or failed workflows cannot be queried.


# Code structure
The following section describes the code in each source file. Any file `name.go` has corresponding unit tests in the file `name_test.go`.

## Parsing Code

- `parsing/cmd_manager.go` - Essentially a public-facing wrapper around `parsing/workflow_execution_state.go` which also handles pattern query evaluation. (For various reasons, it is advisable to keep this separate from the rest of command parsing.)
- `parsing/job_config.go` - Contains formal definitions of the job config files which can be used to customize execution location (this is the config file passed as the second argument to the `run` CLI command).
- `parsing/loose_json.go` - Defines a superset of JSON  where strings can be unquoted. This is used for CLI in parameter substitution, for strings like `-p Start.fastqDir=/path1/path2/path3`. In general, we expect everything following the "=" token in such strings to be a JSON-parsable value, but having to quote everything would be cumbersome.
- `parsing/parse_ows.go` - Contains code for parsing BWB's OWS file format, along with a formal definition of the OWS format's structure.
- `parsing/parse_workflow_cmds.go` - Contains the source for parsing a set of `TypedParams` and a `WorkflowNode` into a set of concrete commands; the `TypedParams` contains the values of all inputs for that node from predecessor nodes or from the workflow definition. The `WorkflowNode` contains info on node iteration, which can cause one command to be produced for each value of the iterable variable; node command structure, including the base command, arguments, flags, and env variables; and substitutions which must be evaluated, if the user includes a `_bwb{VAR}` substitution in the base command.
- `parsing/pickle_to_json.py` - Used only during OWS to JSON conversion in order to read BWB pickle files.
- `parsing/workflow_execution_state.go` - Contains a `WorkflowExecutionState` struct which manages the inputs, outputs, and async behavior of each workflow. If the workflow includes no async behavior, this is all very simple. If it includes async behavior, the workflow is modeled as a tree of async nodes; the "async descendant" nodes, which will execute once for each iteration of their "async ancestor" are subordinate to those async ancestors in the tree. The main purpose of this file is to create the sets of `TypedParams` that are fed as inputs to the code in `parse_workflow_cmds.go` by evaluating links between nodes.
- `parsing/workflow_index.go` - Contains some static information about the workflow graph structure to facilitate quick lookup on which nodes are upstream or downstream of each other in the graph, what the base params of each node are, and how far nodes are from the workflow sink (which is used in scheduling). This index is built right at the start of the workflow parsing and can therefore catch some workflow structure errors before running.

## Workflow code
- `workflow/bwb_workflow.go` - Contains the top-level code for running workflows with various sets of executors. Contains implementations for running workflows both with and without temporal. The main loop here is that the workflow code uses a `CmdManager` instance to generate commands; the workflow code sends the generated commands to an `Executor` (an abstract class that can be either local execution w/ temporal, local execution w/o temporal, or SLURM) and registers a callback for completed commands w/ the executor; and upon command completion, the `Executor` invokes the callback, where the workflow code feeds command outputs to the `CmdManager`, generates successor commands, and executes those.
- `workflow/local_executor.go` - Contains the code for executing commands locally, without temporal.
- `workflow/scheduler_workflow.go` - Contains code for a temporal child workflow which performs scheduling. Job requests (specifying CPU, GPU, and memory consumption) are sent by the main workflow to the scheduling workflow via temporal signals. The scheduler workflow tracks available resources on temporal workers and returns resource grants to the main workflow via temporal signals, which the main workflow subsequently releases via temporal signals. The scheduling workflow also tracks worker liveliness via a heartbeat and registers workers as alive or dead. Scheduling is based on prioritizing jobs with more descendants in the DAG and on prioritizing jobs with larger resource requests.
- `workflow/slurm_poller.go` - Contains code that periodically polls the SLURM node to check for job completion. This is implemented as a temporal child workflow which interacts with the main workflow via signals (similar to the `scheduler_workflow`). Non-temporal SLURM execution is not currently supported.
- `workflow/slurm_executor.go` - Contains code for the SLURM executor, which handles SLURM job submission and tracking, file transfers to the SLURM node, etc. Also starts and manages the slurm polling child workflow.
- `workflow/temporal_executor.go` - Contains code for execution with temporal. Unlike the SLURM executor, this is not a child workflow. Instead, executing a command amounts to executing a temporal activity on a given worker queue (one that was assigned by the scheduler child workflow).
- `workflow/worker.go` - Contains code to start a temporal worker w/ particular CPU, GPU, and memory limits.

## Miscellaneous
- `main.go` - Contains the CLI.
- `docker` directory contains a dockerfile.
- `fs` directory contains some barebones file handling code that I wanted to make into an abstract base class. Upon further reflection, it's very hard to abstract local and remote filesystems into the same interface, so this should probably be refactored out at some point.
