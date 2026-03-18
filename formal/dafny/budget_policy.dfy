// Agentic Knowledge Compiler — Phase 5 correctness slice (Dafny)
//
// This module models a minimal budget/termination policy used by the compile
// controller. It is intentionally small and self-contained so that it can be
// verified quickly in CI via:
//
//   dafny verify formal/dafny/budget_policy.dfy
//
// The goal is to prove simple safety properties about how many LLM calls and
// repair iterations are allowed for a single plan step.

datatype Budget = Budget(
  maxLLMCalls: nat,
  maxRepairsPerStep: nat,
  maxIterationsTotal: nat
)

// Compute the effective maximum number of repair iterations we are allowed to
// perform for a single plan step.
//
// This mirrors the intent of Budget.effective_max_repairs_per_step() in the
// Python controller configuration: the effective repairs must never exceed the
// total iteration budget and must be bounded by the explicit maxRepairsPerStep.
function EffectiveRepairs(b: Budget): nat
  ensures EffectiveRepairs(b) <= b.maxRepairsPerStep
  ensures EffectiveRepairs(b) <= b.maxIterationsTotal
{
  if b.maxRepairsPerStep <= b.maxIterationsTotal then
    b.maxRepairsPerStep
  else
    b.maxIterationsTotal
}

// A simple loop accounting record for a single plan step.
datatype Accounting = Accounting(
  llmCalls: nat,
  repairsUsed: nat,
  iterationsTotal: nat
)

// A state (budget + accounting) is considered "within budget" when:
// - LLM calls do not exceed maxLLMCalls
// - total iterations do not exceed maxIterationsTotal
// - used repairs do not exceed EffectiveRepairs(b)
predicate WithinBudget(b: Budget, a: Accounting)
{
  a.llmCalls <= b.maxLLMCalls
  && a.iterationsTotal <= b.maxIterationsTotal
  && a.repairsUsed <= EffectiveRepairs(b)
}

// A single generate/repair step increases iteration count by exactly 1 and
// optionally consumes one repair iteration. This lemma shows that, provided we
// start in a state that is within budget and we do not exceed the effective
// repair limit, the updated state is still within budget.
lemma lemma_StepPreservesBudget(
  b: Budget,
  a: Accounting,
  consumesRepair: bool
)
  requires WithinBudget(b, a)
  requires a.iterationsTotal < b.maxIterationsTotal
  requires !consumesRepair ==> a.repairsUsed <= EffectiveRepairs(b)
  requires consumesRepair ==> a.repairsUsed < EffectiveRepairs(b)
  ensures WithinBudget(
    b,
    Accounting(
      a.llmCalls,                               // LLM calls unchanged here
      if consumesRepair then a.repairsUsed + 1 else a.repairsUsed,
      a.iterationsTotal + 1
    )
  )
{
}

