# Ops Runbook

## Scope

This runbook covers AKC control-bot operations:

- deployment topology
- required secrets
- policy + approval controls
- incident handling

## Deployment

Run control-bot as a dedicated process/service:

```bash
akc control-bot validate-config --config /etc/akc/control-bot.json
akc control-bot serve --config /etc/akc/control-bot.json
```

Recommended topology:

1. Internet/webhook edge terminates TLS.
2. Reverse proxy forwards only required channel routes to control-bot.
3. control-bot runs with persistent `storage.state_dir` and sqlite path on durable disk.
4. Health probes use `GET /healthz`.

Operational defaults:

- ingress ACK path is fast/asynchronous (bounded queue)
- workers execute commands off-queue
- outbound sends are per-channel rate-limited

## Secrets

Store secrets in a secret manager and inject at deploy time.

Required by channel:

- Slack: `channels.slack.signing_secret`
- Discord: `channels.discord.public_key`
- Telegram: `channels.telegram.secret_token`, `channels.telegram.bot_token` (for outbound replies)
- WhatsApp: `channels.whatsapp.verify_token`, `channels.whatsapp.app_secret`, `channels.whatsapp.access_token`, `channels.whatsapp.phone_number_id`

Do not commit runtime secrets in config files checked into git.

## Policy and Approval

Control layers (in order):

1. identity mapping (`identity.principal_roles`)
2. tenant/workspace routing
3. role allowlist (`policy.role_allowlist`, default deny)
4. optional OPA decision hook (`policy.opa.*`)
5. approval workflow (`approval.requires_approval_action_prefixes`)

Recommended baseline:

- `policy.mode = "enforce"`
- `role_allowlist` contains only action patterns explicitly required by each role
- `approval.requires_approval_action_prefixes = ["incident.", "mutate."]`
- `approval.allow_self_approval = false`

## Intent Quality Contract Operations

When compile or promotion decisions include intent quality checks, triage outcomes in this order:

1. Resolve run scope (`outputs_root`, `tenant_id`, `repo_id`, `run_id`) and open the run manifest:
   - `<outputs_root>/<tenant_id>/<repo_id>/.akc/run/<run_id>.manifest.json`
2. Inspect `control_plane` quality fields:
   - `quality_contract_fingerprint`
   - `quality_overall_score`
   - `quality_dimension_scores`
   - `quality_gate_failed_dimensions`
   - `quality_advisory_dimensions`
   - `quality_policy_reasons`
   - `quality_sidecar_ref`
3. Open the quality sidecar:
   - `<outputs_root>/<tenant_id>/<repo_id>/.akc/run/<run_id>.quality.json`
   - Verify per-dimension scores, evidence refs, gate failures, and advisories match the manifest summary.
4. Review pass-level acceptance evidence:
   - `pass_records[].name == "intent_acceptance"` in the manifest.
   - Step outputs should include `last_intent_acceptance.quality_scorecard` and per-dimension failure/advisory lists.
5. Cross-check indexed aggregates in operations index (`.akc/control/operations.sqlite`) for fleet-level reporting:
   - `quality_contract_fingerprint`
   - `quality_overall_score`
   - `quality_gate_failed_count`
   - `quality_advisory_count`
   - `quality_dimensions_json`

Reason-code interpretation:

- `policy.quality_contract.gate_failed`: one or more dimensions in `enforcement_stage=gate` scored below `gate_min_score`; run is denied/blocked until corrected or thresholds change.
- `policy.quality_contract.advisory`: advisory dimensions scored below `target_score`; run can proceed, but operator review is expected.

### Governance Compile Defaults Example

To enable domain-grounded quality expectations without per-run CLI flags, set policy-bundle governance defaults:

```json
{
  "governance_profile": {
    "version": 1,
    "assurance_mode": "hybrid",
    "verifier_coupling_default": true,
    "verifier_enforcement": "auto",
    "provider_allowlist": [],
    "escalation_thresholds": { "max_errors_before_block": 1 },
    "compile_defaults": {
      "quality_contract_rollout_stage": "phase_b",
      "quality_domain_id": "security_network_secrets",
      "quality_domain_matrix_path": "tests/fixtures/knowledge_domains/domain_coverage_matrix.json"
    }
  }
}
```

Behavior:

- `quality_contract_rollout_stage` controls advisory vs gate defaults (`phase_b`/`phase_c` enable critical gates).
- `quality_domain_id` + `quality_domain_matrix_path` load `quality_evidence_expectations` from the selected domain in the matrix.
- loaded expectations are persisted into compile metadata and applied when the default quality contract is auto-injected.

## Audit and Logs

Primary audit stream:

- `storage.audit_log_path` (or default `<state_dir>/control_bot_audit.jsonl`)
- structured event types:
  - `control.bot.command.received`
  - `control.bot.command.denied`
  - `control.bot.command.approval_requested`
  - `control.bot.command.approved`
  - `control.bot.command.executed`
  - `control.bot.command.failed`

Mutation mirroring:

- successful key mutate/incident/approval actions also append tenant control audit rows under:
  - `<outputs_root>/<tenant_id>/.akc/control/control_audit.jsonl`

## Incident Handling

When operator command incidents occur:

1. Confirm ingress auth failures vs policy denials (HTTP logs + audit stream).
2. Inspect pending approvals in sqlite (`approval_requests` table).
3. Review command outcomes (`command_results`) and inbound dedupe records (`inbound_events`).
4. Pull tenant control audit (`control_audit.jsonl`) for mutation timeline.
5. If compromise suspected:
   - rotate all channel secrets/tokens
   - disable affected channel (`channels.<name>.enabled=false`)
   - tighten role allowlist / OPA policy and redeploy
6. Re-enable channel only after validation:
   - `akc control-bot validate-config`
   - signed webhook checks passing
   - test command path in a non-production tenant/workspace

## Recovery Notes

- Queue backpressure (`queue_full`) indicates ingress saturation or downstream channel/API slowdown.
- Scale by increasing `worker_threads` carefully and keep rate limits conservative.
- Keep sqlite on fast local disk; backup state for approval/result forensic continuity.
