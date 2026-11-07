# Opt-in Workbench hybrid execution

The new `hybrid`, `cloudexec`, `transfer`, `durable`, and `executioncontract`
packages implement a separate versioned Temporal path. Legacy workflow parsing,
command-manager routes and recorded histories remain compatible.

Build with the module's Go 1.24.4 toolchain. New commands:

- `hybrid-workers --config worker.json --queues queue1,queue2`: serve only explicit
  site/controller queues on the correct data host.
- `artifact-transfer --request-base64 ...`: pinned data client invoked by a trusted
  stage runner; it does not grant public submission authority.
- `bundle-run --archive inputs/bundle.tar --destination bundle -- /bin/bash run.sh`:
  safely extract an explicitly declared bundle input and run one reviewed stage.

The API requires `BWB_HYBRID_CONFIG` and `BWB_API_BEARER_TOKEN`. Workflows require
a dedicated namespace and queues configured using `BWB_TEMPORAL_ADDRESS` and
`BWB_TEMPORAL_NAMESPACE`. `/capabilities` advertises opt-in support. The trusted
Workbench service owns durable human approval and sends an immutable request to
`/start_hybrid_workflow`. `/validate_hybrid_plan` is read-only and returns policy
and execution-configuration pins. `/hybrid_workflow_status`, `/hybrid_logs`,
`/cancel_hybrid_workflow`, `/hybrid_collect`, `/hybrid_reconcile`, `/hybrid_artifacts`, and
`/hybrid_artifact_access` enforce stored owner linkage.

The API and AWS controller workers can run locally against the existing Temporal
service. `AWS_PROFILE` selects the operator's source credentials. AWS profiles
with explicit `controller_auth_mode: "default_chain"` use them directly and must
omit `controller_role_arn`. An omitted mode, or explicit `assume_role`, requires
that ARN and uses temporary role credentials. There is no fallback on denied role
assumption. Mode changes require updated immutable profile pins and job review.
Gateway transfers and artifact-access signing use the default SDK chain separately;
EC2 transfers use instance-profile credentials. Access-key profiles work for initial
tests; Identity Center profiles can later use `aws sso login` with provider-enforced
MFA. The worker never prompts interactively for MFA during background execution.

Install `cloudexec/runner/runner.py`, this binary, and a hashed runtime receipt on
approved Linux executors. Resource journals require persistent POSIX storage,
shared with controller replicas and supporting flock/fsync/atomic rename.
Do not remove original worker queues or profile revisions while jobs or retained
resources still need lifecycle operations. Reconciliation is dry-run by default;
apply checks process and mount evidence and mutates only exact owned resources.

The paired Workbench checkout contains detailed installation, IAM/service/config
examples, native-host authorization, recovery and live-test instructions in
`docs/AWS_HYBRID_RUNTIME_V1.md` and `deploy/aws-hybrid/`.

`go test ./...` includes fake AWS/S3/Slurm and real subprocess runner tests, plus
recorded-history replay. Recording new fake-activity histories requires explicit
`BWB_RECORD_HYBRID_HISTORY=1` against localhost Temporal in the dedicated
`workbench-hybrid-offline-v1` namespace. These are not live AWS test results.
The user-selected `uw` profile passes real STS/EC2/SSM reads through explicit
default-chain authentication, and real small/multipart S3 round trips passed
with exact object cleanup. EC2 scientific execution and Slurm qualification
still need the approved site/runtime configuration. No EC2 instance or IAM
policy has been changed; synthetic S3 objects were written and deleted.

Plans may include `checkpoints` with reviewed milestone `after_nodes`, blocked
`before_nodes`, a description and mode `automatic` or `manual`. Automatic
conditions are reviewed QC commands: all milestones must finish successfully
and publish their evidence. Manual mode additionally requires
`review_timeout_seconds` (60 seconds to 30 days). A failed QC stage stops the
workflow; approval cannot override it. Each milestone must be an ancestor of
every blocked root, preventing gate deadlocks and early release.

`Run` publishes checkpoint states through its existing `status` query. Pending
manual reviews retain immutable output receipts, a plan/evidence hash and an
expiry, without starting blocked child workflows or allocating their EC2 nodes.
The owner-bound `/hybrid_checkpoint_decision` service route delivers a durable,
deduplicated signal. Only the Workbench human adapter creates its receipt;
there is no model-callable approve argument. A changed evidence hash, expired
review, conflicting decision or unknown owner is rejected. Earlier approved
plans without checkpoints preserve their existing histories.

The default suite replays `history_checkpoint_v1.json`, recorded with fake
scientific activities in the isolated local namespace. The recording stopped
the worker at a checkpoint, delivered approval while it was offline, restarted
it and verified successful continuation. Re-recording requires explicit
`BWB_RECORD_CHECKPOINT_HISTORY=1`; it performs no AWS calls.
