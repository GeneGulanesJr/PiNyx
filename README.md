# PiNyx
from Nyktos (Greek for “night”/darkness), evokes something hidden, compact, and mysterious.
## Model Gateway / Router Overview Plan

### 1. Purpose

The **Model Gateway** is the only system allowed to call AI models.

Agents, orchestrators, tools, and runtimes should not call providers directly.
Everything goes through the gateway.

Its job is to choose the right model, control cost, log usage, and prevent unsafe model calls.

---

## 2. Main Idea

```txt
User / Agent / Orchestrator
        ↓
Model Gateway
        ↓
Local Intent Classifier
        ↓
Rules + Budget + Policy
        ↓
Selected Model Provider
```

The classifier only detects intent.
The gateway makes the final decision.

---

## 3. Core Behavior

User-facing requests should usually use a **good model** because quality matters most there.

Internal coding and implementation tasks should usually use a **cheaper coding model** first.

So the default logic is:

```txt
User request → good reasoning model
Planning → good reasoning model
Architecture → good reasoning model
Validation → good reasoning model

Implementation → cheaper coding model
Small code edits → cheap coding model
Repetitive edits → cheapest coding model
Debugging → mid/strong coding model
Failed cheap attempt → escalate
Risky action → good model + permission check
```

---

## 4. Local Classifier

The classifier can be a small local model.

It does not need deep reasoning.
It only needs to classify intent.

Example labels:

```txt
user_chat
planning
architecture
coding
debugging
refactor
summarization
validation
research
tool_request
risky_action
long_context
```

The classifier should not directly choose the final model.
It only gives a signal.

---

## 5. Rule Engine

Rules override the classifier when something is obvious.

Examples:

```txt
If source = user
  prefer good model

If source = internal_agent and task = coding
  prefer cheap coding model

If task failed before
  escalate one tier

If context is huge
  require long-context model

If task modifies files
  mark as tool/action task

If task can delete/change/deploy
  require permission gate

If budget is low
  prefer cheaper model unless risk is high
```

---

## 6. Model Registry

The gateway needs a registry of available models.

Each model should have metadata like:

```txt
name
provider
cost tier
speed
context limit
strengths
supports tools
supports vision
supports code
supports long context
supports structured output
reliability level
```

The router chooses from this registry instead of hardcoding model names everywhere.

---

## 7. Fallback Logic

Every model decision should include fallback behavior.

Example:

```txt
cheap coding model fails
  → retry once
  → escalate to stronger coding model

strong reasoning model unavailable
  → use backup reasoning model

provider rate limited
  → switch provider

output invalid
  → retry with stricter format
```

This keeps agents reliable without always starting with the expensive model.

---

## 8. Cost and Usage Control

The gateway should track:

```txt
input tokens
output tokens
estimated cost
actual cost
model used
provider used
task type
agent/source
mission/project
success/failure
fallbacks used
```

This is important for Observability later.

---

## 9. Permission Connection

The Model Gateway does not execute tools, but it should flag risk.

Example:

```txt
normal answer → no permission
code generation → no permission
file edit suggestion → no permission
actual file edit → permission needed
shell command → permission needed
delete/deploy/database change → permission needed
```

The Permission System decides approval, but the gateway helps classify risk.

---

## 10. Recommended Stack

Use **Rust** for the gateway core.

Rust is good for:

```txt
routing logic
provider adapters
local classifier calls
budget checks
logging
concurrency
retries
rate limits
stable daemon behavior
```

Use **Bun/TypeScript** for:

```txt
UI
dashboard
desktop app shell
API client
developer controls
```

---

## 11. Final Architecture

```txt
Model Gateway
├─ Request Intake
├─ Local Intent Classifier
├─ Rule Engine
├─ Model Registry
├─ Budget Manager
├─ Provider Adapters
├─ Retry/Fallback Manager
├─ Risk Flagger
├─ Token/Cost Logger
└─ Memory/Observability Event Writer
```

---

## 12. Simple Summary

The Model Gateway is the **control point for all AI usage**.

It makes sure:

```txt
User-facing thinking uses good models.
Internal coding uses cheaper models.
Risky tasks are flagged.
Failed tasks escalate.
Every model call is logged.
No agent talks directly to providers.
```
