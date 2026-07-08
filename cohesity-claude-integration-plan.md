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

## ACU metering — how usage is actually measured

Your org meters Claude Code usage in **ACUs**, not raw tokens:

```
ACU = (effective_input + effective_output) × model_weight

effective_input  = input_tokens × 1.0
                  + cache_read × 0.1
                  + cache_write_5m × 1.25
                  + cache_write_1h × 2.0

effective_output = output_tokens × output_ratio
```

| Model | Weight | Output ratio |
|---|---|---|
| Claude Haiku | **0.33** | 5x |
| Claude Sonnet | 1.0 (baseline) | 5x |
| Claude Opus | 1.67 | 5x |

(Non-Claude models not listed here for brevity — see the org reference doc.
Unlisted models default to 1.0.)

| Token type | Multiplier | Why |
|---|---|---|
| Uncached input | 1.0x | base price |
| Cache read (hit) | **0.1x** | 90% cheaper than base |
| Cache write (5 min) | 1.25x | 25% surcharge to cache |
| Cache write (1 hour) | 2.0x | 100% surcharge to cache |
| Output tokens | **5x** (Claude) | output is 5x more expensive than input, before model weight |

**Cost reference:** 1 ACU ≈ $0.000003 (Sonnet input pricing, $3/MTok). Current
limit: **3,200,000 ACU ≈ $9.60 per window**.

**Throttle windows:** a 5-hour window (resets 00:00, 05:00, 10:00, 15:00,
20:00 ET) and a weekly window (resets Monday 00:00 ET), tracked
simultaneously. **Crossing either threshold sends an alert to your Sr
Director** — this is the part that matters for the justification doc: your
efficiency claim isn't self-reported, it's the same number your management
chain already sees.

### What this math actually implies for daily use

1. **Output length is the single biggest lever, not model choice.** Output
   tokens are weighted 5x before the model multiplier even applies. Asking
   for a 150-word bullet summary instead of a 600-word narrative is a ~4x
   ACU cut on the output term regardless of which model answers.
2. **Default to Haiku for routine/scheduled work.** Haiku's 0.33 weight vs.
   Sonnet's 1.0 is a 3x discount on identical token counts — appropriate for
   pattern/anomaly narration over a small digest (Tier 3 above), where the
   task isn't reasoning-heavy.
3. **Caching pays off on repeated reads within a window, not one-shot
   calls.** A cache write costs *more* than uncached input (1.25x–2x) and
   only becomes worth it once you've read it back a few times at 0.1x. That
   fits interactive troubleshooting (many turns, same system context) far
   better than a once-a-day scheduled report (one read, no payback).
4. **Never feed raw API JSON — this is the same rule as the digest-first
   pipeline above, now quantified.** A raw Helios payload as uncached input
   at 1.0x, summarized by Opus (1.67x) with a verbose response, is easily
   two orders of magnitude more ACU than the same task done as: cached
   digest read (0.1x) → Haiku (0.33x) → a short, structured answer.

### Illustrative comparison (placeholder token counts — replace with real numbers after piloting)

| Approach | Input treatment | Model | Output | Rough ACU |
|---|---|---|---|---|
| Naive: raw JSON, verbose answer | ~50,000 tokens uncached (1.0x) | Opus (1.67) | ~800 tokens × 5 | ~90,000 |
| Disciplined: digest, cached, terse | ~800 tokens, cache-read steady state (0.1x) | Haiku (0.33) | ~150 tokens × 5 | ~270 |

That's roughly two orders of magnitude apart on the same underlying task —
not because Claude is "smarter" one way or the other, but because of the
pipeline (digest-first) and habits (short output, right-sized model)
described in this plan. Don't quote this ratio to your Sr Director as
measured fact; run the pilot, pull the real ACU numbers from the dashboard,
and swap them in — the methodology is what's solid, not these placeholders.

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

## Daily operations: where Claude fits

You mentioned your day is really: management reporting on backup failures,
CR (change request) tasks, incidents, maintenance, patching, troubleshooting,
Confluence, and Jira. Not all of these need a pipeline — most don't. Here's
each one, honestly, with a token-cost tier and whether it needs any setup:

| Work stream | Claude's role | Token tier | Setup needed |
|---|---|---|---|
| **Backup-failure / management reporting** | Narrate the digest into a plain-English summary + anomaly flags (this is the Tier 1→2→3 pipeline above) | Low, scheduled | Yes — the pilot above |
| **Troubleshooting** (scripts, API errors, Cohesity/Dynatrace weirdness) | Interactive Claude Code session — paste the error/script, work it live | Medium, but occasional/bursty | **None — start today** |
| **CR (change request) write-ups** | You give it bullet points (what's changing, why, blast radius); it drafts the description, risk section, rollback plan | Low-medium, per-CR | None — copy/paste workflow |
| **Maintenance & patching** | Draft/refresh runbooks and pre/post checklists; summarize vendor patch/release notes into "what changed, what's our risk" | Low, periodic | None |
| **Incidents** | Draft a timeline summary or stakeholder update from facts you already have (ticket notes, the failure digest) — **not** from raw logs; use it live for troubleshooting during the incident too | Medium, event-driven | Define a small incident digest format first (Phase 4 below) |
| **Confluence** | Turn rough notes/meeting outcomes into a clean page; keep docs in sync with what a script actually does | Low, occasional | Manual copy/paste works fine; check for an Atlassian MCP connector if you want it wired in directly |
| **Jira** | Draft ticket descriptions/acceptance criteria; summarize a backlog for standup | Low, occasional | Same as Confluence — connector optional, not required |

**Where to actually start, in order:** troubleshooting first (zero setup,
useful immediately), then the backup-failure reporting pilot (already
planned, highest recurring value), then CR/maintenance/patching drafting
(just paste bullets in — no pipeline to build), then incidents once you have
a digest format, and Confluence/Jira last since those depend on whether a
connector exists in your environment.

## Before rollout to the client repo

- Resolve the `_do_not_delete` / `_test` / `_prod_test` duplication — even
  just a `README` per folder or a `production/` vs `sandbox/` split. This is
  independent of Claude but directly reduces the "which file is real"
  overhead any future Claude Code session (or teammate) pays.
- Decide where digests live (e.g. `reports/<name>/latest.md`) so Tier 3
  prompts can hardcode paths instead of searching.
- Know your ACU budget going in: 3,200,000 ACU per window (5-hour and weekly
  windows tracked simultaneously), ≈ $9.60/window. Crossing it alerts your Sr
  Director automatically — build habits (Haiku-first, digest-first, terse
  output) that keep routine daily use a small fraction of that, and reserve
  headroom for occasional Sonnet/Opus investigation work.

## What this plan deliberately does NOT do

- It does not have Claude call the Helios/Dynatrace APIs directly — no
  benefit, and it's the fastest way to blow through a metered budget on raw
  JSON.
- It does not replace PowerBI — PowerBI stays the dashboard/trend-chart
  layer; Claude's job is the narrative/anomaly layer that's awkward to
  express in a BI tool.
- It does not require rewriting existing production scripts. Tier 1 is
  frozen; only a thin Tier 2/3 gets added.

## Progress checklist

Check these off as you go — this is the running status for the rollout.

**Phase 0 — Setup**
- [x] Learn the org's ACU metering formula, model weights, cache multipliers, and throttle windows
- [ ] Confirm individual (not shared) login so ACU usage is attributable to you specifically
- [ ] Note which folders are live vs. `_do_not_delete`/`_test` (even a one-line README per folder) so a future `CLAUDE.md` has something accurate to point to

**Phase 1 — Pilot: backup-failure reporting**
- [ ] Confirm `dyna_backup_failure`'s `markdownTable` output as the Tier-2 digest (no new collector code needed)
- [ ] Write the fixed Tier-3 prompt template for the daily narrative
- [ ] Wire a scripted `claude -p` (Haiku) call after the Dynatrace workflow
- [ ] Run it alongside the existing raw table for ~2 weeks and sanity-check the narrative against ground truth
- [ ] Measure actual token/cost spend on this one flow before expanding

**Phase 2 — Everyday ad hoc use (no pipeline required)**
- [ ] Use interactive Claude Code for troubleshooting scripts/errors as they come up
- [ ] Draft CR descriptions and rollback plans from bullet notes
- [ ] Draft/refresh maintenance and patching runbooks and checklists

**Phase 3 — Expand reporting**
- [ ] Apply the same Tier-2/Tier-3 pattern to a second report (capacity or interface status)
- [ ] Add day-over-day diffing as a deterministic script step (not an LLM call), feeding the "what's new since yesterday" into the narrative prompt

**Phase 4 — Incidents**
- [ ] Define a lightweight incident digest format (timeline + key facts — not raw logs)
- [ ] Use Claude to draft RCA/stakeholder-update drafts from that digest
- [ ] Use Claude interactively during live incidents for troubleshooting

**Phase 5 — Confluence & Jira**
- [ ] Check whether an Atlassian (Confluence/Jira) MCP connector is available in your environment
- [ ] If yes: pilot auto-drafting one doc page or one ticket type
- [ ] If no: keep using manual copy/paste (draft in Claude, paste into Confluence/Jira) — still useful, zero integration risk
