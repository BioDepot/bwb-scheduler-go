# Offline execution contracts

This package implements Gate 1 of the Workbench Temporal/EC2 runbook. It validates
managed/existing EC2 configuration, EBS/S3 storage, per-node placement, immutable
profile references, trusted worker policy, review fingerprints and retry IDs.
It makes no AWS or Temporal calls and does not enable hybrid execution.

`Validate` parses the versioned contracts; `Authorize` additionally checks a plan
against an operator-loaded policy. `ExecutionIDs` derives stable logical command
attempt identifiers. `SubmissionRecord` and `CheckRetry` support persisted request
deduplication; callers must store the original job reference atomically. Hashes
are content evidence, not authentication or proof of human approval.

Workbench owns the normative `morphic_agent/schemas/execution-contracts-v1.json`
and `tests/fixtures/execution-contracts-v1.json`. This package mirrors them as
`schema.json` and `testdata/contracts-v1.json`. Run Workbench's
`scripts/check_execution_contract_mirror.py <scheduler-checkout>` when editing
either copy. The Workbench `docs/AWS_EC2_CONTRACTS_V1.md` explains the fields,
canonical hashing, trust boundaries, limits, and follow-on gates.

Run `go test ./executioncontract ./server ./parsing ./fs ./workflow ./staged`
with Go 1.24.4. Existing scheduler configuration and retry behavior remain intact.
The start endpoint rejects `execution_plan` until the new placement runtime is
implemented, preventing legacy workers from silently ignoring a hybrid plan.
