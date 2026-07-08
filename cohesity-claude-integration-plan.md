# Claude Code Integration Plan — Cohesity/Dynatrace Operations

## Current state (from this repo)

- **Collectors** (PowerShell + Dynatrace custom-app JS) call the Cohesity Helios
  API directly per cluster/protection-group and produce either CSV
  (PowerShell, e.g. interface status) or a markdown table + JSON
  (Dynatrace JS, e.g. `dyna_backup_failure`).
- **Distribution** is Dynatrace workflows (email) and PowerBI dashboards fed
  from CSV/exports.
- The repo has a lot of `_do_not_delete`, `_test`, `_prod_test` duplicates —
  normal for iterating live, but it costs tokens every time Claude has to
  re-figure-out which copy is real.

**Key fact this plan leans on:** the collectors already do the expensive part
(API calls, filtering, joining, formatting) for free — no LLM involved. That
work should stay exactly where it is.

## Guiding principle

> Claude should never see raw Helios/Dynatrace API responses. It should only
> ever see the already-aggregated output your scripts already produce.

A `dyna_backup_failure` run today ends in a markdown table typically a few KB.
That's the right size to hand an LLM. The underlying Helios JSON (all runs,
all objects, all failed attempts, across every cluster) can be hundreds of KB
— that's the wrong size, and it's what would blow up token usage if Claude
were ever wired to call Helios itself.

## Three-tier pipeline

```
[1] COLLECT              [2] DIGEST                [3] INSIGHT
(unchanged scripts)   →  (new, deterministic)   →  (Claude, scripted, non-interactive)
PowerShell / Dynatrace    trim + dedupe + cap        claude -p with a fixed prompt
JS → Helios API            rows, produce a small       template, reading ONLY the
                            JSON/markdown digest         digest file
```

- **Tier 1** stays as-is. Don't rewrite working PowerShell/Dynatrace scripts
  to "add AI" — that's where the metered budget would leak.
- **Tier 2** is new but cheap: a small step (PowerShell function or a few
  lines in the existing JS) that caps the digest — e.g. top 20 failures by
  recency, one row per PG/RunType (you already mostly do this), strip
  anything over ~300 chars per field (also already done in
  `toMarkdownTable`). Output goes to a fixed path/filename per report type.
- **Tier 3** is the only place Claude touches anything, and it only reads the
  Tier 2 digest — never the API, never the full script output.

## Token-control tactics specific to Claude Code

1. **Non-interactive, scripted calls for routine ops.** Use
   `claude -p "<prompt>" < digest.md` (or `--print`) from a scheduled task,
   not interactive sessions. Interactive sessions accumulate turns and
   re-read context; a scripted one-shot call has a bounded, predictable cost.
2. **Model tiering.** Route by task:
   - Daily failure/capacity digest → summarize with the cheapest model
     (Haiku) — it's pattern/anomaly narration over a small table, not
     reasoning-heavy.
   - Weekly/monthly trend analysis, root-cause investigation, or anything
     that needs to read multiple digests together → Sonnet, run less often.
   - Reserve interactive Claude Code sessions (any model) for script
     development/debugging, not for the daily insight generation itself.
3. **Fixed prompt templates, not free-form chat.** Write the insight prompt
   once per report type (e.g. "Given this backup-failure table, flag any PG
   failing 3+ consecutive days, note new failures vs. the same table
   yesterday, keep it under 150 words"). A fixed template avoids re-deriving
   instructions — and is a natural place to use prompt caching if the SDK/CLI
   supports it for a stable system prompt across daily runs.
4. **Day-over-day diffing stays deterministic.** "Is this failure new since
   yesterday" is a data problem (compare two digests), not an LLM problem —
   do it in PowerShell/JS before Claude ever sees it. Ask Claude to
   *narrate* the diff, not *compute* it.
5. **A `CLAUDE.md` at the repo root** once this moves to the client repo,
   documenting: which folders are live vs. `_do_not_delete`/test, the digest
   file locations/formats, and the prompt templates. This is the single
   biggest lever for a repo this size — without it, every Claude Code session
   burns tokens re-exploring ~70 ambiguously-named folders to figure out
   what's current. Worth doing before wiring anything else up.
6. **Cap what Claude Code can read.** Point sessions at the specific digest
   file, not the whole repo. If Claude Code is ever given filesystem access
   in the client environment, scope it to a `digests/` or `reports/`
   directory rather than the full script tree.

## Pilot recommendation

Start with **one** flow end-to-end before generalizing: the backup-failures
report (`dyna_backup_failure` / `cohesity_interface_Details_Do_not_del`
lineage), since it already emits a clean markdown table.

1. Confirm the Dynatrace JS output (`markdownTable`) is the Tier-2 digest —
   it already is, no new code needed.
2. Add one scripted step after the Dynatrace workflow: pipe that markdown
   into `claude -p` (Haiku) with a fixed prompt asking for a short narrative
   summary + flags for repeat/new failures.
3. Feed the narrative into the existing email/PowerBI distribution alongside
   (not instead of) the raw table, so you can sanity-check Claude's read
   against the ground truth for a couple weeks.
4. Measure actual token spend on this one flow before adding a second
   report type (capacity, interface status, etc.).

## Before rollout to the client repo

- Resolve the `_do_not_delete` / `_test` / `_prod_test` duplication — even
  just a `README` per folder or a `production/` vs `sandbox/` split. This is
  independent of Claude but directly reduces the "which file is real"
  overhead any future Claude Code session (or teammate) pays.
- Decide where digests live (e.g. `reports/<name>/latest.md`) so Tier 3
  prompts can hardcode paths instead of searching.
- Set a Bedrock budget/alarm for the Claude Code usage specifically, since
  it's metered separately from the rest of the AWS bill.

## What this plan deliberately does NOT do

- It does not have Claude call the Helios/Dynatrace APIs directly — no
  benefit, and it's the fastest way to blow through a metered budget on raw
  JSON.
- It does not replace PowerBI — PowerBI stays the dashboard/trend-chart
  layer; Claude's job is the narrative/anomaly layer that's awkward to
  express in a BI tool.
- It does not require rewriting existing production scripts. Tier 1 is
  frozen; only a thin Tier 2/3 gets added.
