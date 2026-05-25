# Claude Code — Initial Prompt: Distributed KV Cache (Learning Project)

> Paste this into Claude Code while in **planning mode**. Before sending, update the path in the "Context" section to point at wherever you actually placed the project plan in this repo.

---

## Context

I'm building a **distributed key-value cache for LLM inference**: a cache layer that sits alongside vLLM and shares attention KV tensors across a multi-node cluster, using consistent hashing, replication, failover, a cost-aware + fair eviction policy, deployed on AWS via Terraform.

The complete strategy, architecture sketch, phased plan, tech decisions, risk register, and interview framing are in the project plan I've added to this repo: **`./distributed_kv_cache_project_plan.md`** (adjust path if different). **Read it in full before doing anything else.** Treat it as the source of truth for scope and decisions. If you spot a problem, contradiction, or something that no longer makes sense, flag it rather than silently diverging.

## The most important framing: this is an EDUCATIONAL project

This is **not** production software, and speed-to-shipping is **not** the goal. The goal is for *me* to deeply learn:

- Software architecture and design
- The full software development lifecycle
- Distributed systems — and how to implement them in Go
- LLM inference internals and KV caching (integration with vLLM)
- Cloud deployment and infrastructure-as-code (AWS + Terraform)
- Good SWE practices (testing, documentation, decision records, git hygiene)

I am **new to Go, distributed systems, and LLM serving.** Optimize every choice for my understanding, not for reaching a working binary fastest. If a slower path teaches me more, prefer it.

## How I want us to work together

### Guided implementation (NOT autocomplete) for the core

For the substantive parts of the system, **do not write the implementation for me.** Instead, follow this teaching loop for each non-trivial component:

1. **Teach** — explain the concept and why it matters here (e.g., what problem consistent hashing actually solves for us, not just how to code it).
2. **Design** — lay out the approach options and their tradeoffs; we decide together.
3. **Skeleton** — give me interfaces, function signatures, and struct definitions, plus a stub with `TODO`s and guidance comments — but **not** the filled-in logic.
4. **I implement** — I write the actual logic myself.
5. **Review** — review what I wrote, point out bugs and improvements, and ask me a question or two to check that I actually understand it.
6. **Capture** — we record the decision (ADR) and what I learned (learning log).

If I get stuck, give me a hint or a leading question *before* handing me the answer.

### Where direct generation IS welcome

To avoid burning time on mechanical work, you can generate these directly — just briefly explain what you generated and why:

- Project scaffolding and directory structure
- Build/config/boilerplate: `go.mod`, `Makefile`, `Dockerfile`, Terraform boilerplate, CI config
- Interface and type definitions and stubs — **after** we've agreed on the design
- Repetitive or mechanical code, and test scaffolding

### The dividing line

**I implement with your guidance (the [guided] work):** the consistent-hashing ring, replication and failover logic, leader-election / etcd integration, the eviction policy (especially the cost-aware + fairness policy in Section 3.5 of the plan), gRPC handler logic, the interesting parts of the synthetic load generator, and the chaos-test logic.

**You may scaffold directly (the [scaffold] work):** everything in the "direct generation" list above.

When you're unsure which side of the line something falls on, ask me rather than assuming.

## Documentation deliverables

Documentation is a **first-class deliverable**, not an afterthought — it's a primary way I learn, and a record I'll later mine for a blog post and interview prep. Here's the structure I'm proposing; in planning mode, confirm it or suggest improvements before we create anything:

```
README.md                          # entry point: what this is, how to navigate, current status
docs/
  01-architecture.md               # system design, component + data-flow diagrams, key data structures, the "why"
  02-roadmap-and-workflow.md       # phased dev plan (from the project plan), milestones, definition-of-done, our working agreement
  03-distributed-systems-in-go.md  # concepts (sharding, replication, consensus, failover) + how we implement them in Go, with patterns
  04-kv-cache-and-vllm.md          # what a KV cache is, how vLLM works, our integration + the KV transfer protocol
  05-cloud-deployment-aws.md       # AWS + Terraform: how to deploy, IaC walkthrough, cost discipline
  adr/                             # Architecture Decision Records: one short file per significant decision (why, alternatives, tradeoffs)
  learning-log.md                  # running log: per phase, what I learned, what broke, what I'd redo (feeds the eventual blog post)
```

Guidelines for the docs:

- Use **Mermaid diagrams** for architecture, sequence/data flow, and state machines, so the design is visual and not just prose.
- Keep docs **living** — update them as we build; don't write them once and let them rot.
- Docs should **teach, not just describe** — explain the *why*, including alternatives we considered and rejected.
- **ADRs** capture decisions as we make them. The project plan already has a decisions log you can use to seed the initial ADRs.
- Don't duplicate the project plan into the docs — **complement** it. The plan is the strategy; these docs are the working/implementation/learning layer.

## What I want from you in THIS planning session

**Do not write any project code yet.** In planning mode, produce:

1. A short confirmation that you've read the project plan, plus any issues, risks, or gaps you noticed.
2. A proposed (or refined) documentation structure.
3. A proposed phase-by-phase execution plan derived from the project plan — starting with **Phase 0** — broken into concrete, reviewable steps, with each step tagged **[guided]** (I implement) or **[scaffold]** (you generate).
4. A proposed Claude Code setup per the side note below: what goes in `CLAUDE.md`, what becomes a `.claude/rules/` file, and what becomes a Skill — so our rules persist and the context budget stays lean from day one.
5. A restatement of our working agreement (the guided-vs-scaffold split) so we're aligned before building.

Then **stop and wait for my approval** before creating any files or writing any code.

## One more note

I'm separately following a pre-project foundations learning plan (Go, distributed-systems basics, gRPC, AWS/Terraform setup) before and during this build, so assume I'm actively ramping on these. When you hit a concept I likely don't know yet, **teach it briefly inline** rather than assuming familiarity — a couple of sentences and a pointer is enough, then continue.

---

## Side note: use Claude Code's own best practices to manage context and memory

Set this project up using Claude Code's native mechanisms so our rules persist and our token budget stays focused on reasoning, not on re-loading boilerplate every turn. Specifically:

**1. Create a `CLAUDE.md` to hold our durable rules.** This file loads at the start of every session, so put the *always-true, every-session* facts here and nothing else. For this project that means: the educational framing and the guided-vs-scaffold working agreement (the core behavioral rule), build/test/run commands, the project layout, Go and naming conventions, git workflow, and a one-line pointer to the project plan as source of truth. Keep it **under ~200 lines** — it's context on every turn, and longer files both cost tokens and reduce how reliably you follow them. Write rules concretely and verifiably ("run `go test ./... -race` before every commit," not "test your code"). You can run `/init` to scaffold a first draft from the repo, then I'll refine it. Use HTML comments (`<!-- ... -->`) for any maintainer notes you want kept out of the context budget.

**2. Use `.claude/rules/` for instructions that only matter sometimes.** Anything that's topic- or path-specific — Terraform conventions, gRPC/proto style, test conventions — should be a separate file in `.claude/rules/`, ideally with `paths:` frontmatter so it loads *only* when we're touching matching files. This keeps `CLAUDE.md` lean while still having detailed guidance available exactly when relevant.

**3. Use Skills for the heavy, load-on-demand reasoning.** This is the token-saving pattern I care about most: a Skill (a `SKILL.md` plus supporting files) is **not** loaded at launch — it's pulled in only when invoked or when you judge it relevant. So the deep, verbose material — e.g. a "distributed-systems-in-Go implementation playbook," a "vLLM integration procedure," or a "cloud-deploy runbook" — belongs in Skills, not in CLAUDE.md. The `SKILL.md` stays short and points to the detailed files, which load on demand. Net effect: the depth of reasoning and detail is fully preserved, but it only consumes context when we're actually doing that kind of work. This is the right home for multi-step procedures and for the teaching content of the per-sector docs. (If `/init` offers an interactive setup that includes skills and hooks, use it.)

**The dividing line for the three:** always-on rules → `CLAUDE.md`; load-when-relevant rules → `.claude/rules/`; load-on-demand procedures and playbooks → Skills. When you're unsure where something belongs, prefer the more on-demand option to protect the context budget, and ask me if it's a judgment call.

**4. Lean on auto memory, but verify it.** Claude Code's auto memory will accumulate build commands, debugging insights, and patterns across our sessions on its own. That's useful for a long multi-week project — but I want to *see* what it records, so periodically tell me when you've written something to memory, and I may run `/memory` to audit it. Don't let auto memory silently encode a wrong assumption.

**5. General agentic-coding hygiene I want us to follow:**
- **Plan before code.** Use planning mode for any non-trivial change; show me the plan and wait for approval before editing. (This whole first session is plan-only.)
- **Small, reviewable steps with frequent commits.** Tight commits with clear messages over big batches — easier for me to learn from and to review. Follow a consistent commit convention (note it in `CLAUDE.md`).
- **Close the loop with verification.** After a change, run the build/tests/linter yourself and report results; treat a task as done only when it's verified, not just written.
- **Use hooks for must-always-happen steps.** If something truly must run at a fixed point (e.g. format/lint before every commit), prefer a hook over a CLAUDE.md instruction, since hooks execute deterministically rather than relying on you remembering.
- **Keep the docs and the decision records (ADRs) updated as we go**, not in a batch at the end — they're part of "done."
- **Surface uncertainty.** If you're guessing about an API, a Go idiom, or an AWS detail, say so and verify rather than confabulating — I'd rather a checked answer than a confident wrong one.

Treat this side note's setup (CLAUDE.md + rules + skills structure) as part of what you propose in this planning session, before we write any project code.
