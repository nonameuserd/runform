# Golden IaC workspace snapshot (inspectable, checked-in)

This directory is a **no-run** snapshot of what the `infrastructure_synthesis` pass emits.

- `infra_plan.json` and `iac_manifest.json` are top-level copies for quick inspection.
- The full emitted workspace tree is under `.akc/infra/run-demo/` (Terraform JSON + AWS CDK TypeScript).

This is intentionally **inspectable evidence**, not an instruction to apply it.

Relevant command surface (in real runs):

```bash
akc provision plan --project-dir . --run-id run-demo --backend terraform --environment staging
```

(Provisioning requires tool availability and readiness gates; this snapshot exists so operators can review without running compile.)

