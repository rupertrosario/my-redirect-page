# Why I should pilot Claude Code — and how I'll keep usage low

*Draft for your Sr Director. Fill in `[bracketed]` items with real specifics
(your name, team name, exact CR volume, actual ACU numbers once you have
them) before sending — everything here is either a fact you gave me or is
explicitly marked as an illustrative placeholder, not a real measurement.*

## The ask

I'd like to pilot Claude Code for a defined scope of my backup
administration work, using my individual login so usage is attributable to
me specifically, with a review after [2-4 weeks] against our team's ACU
budget.

## Why me, and why now

I'm proposing myself as the pilot user rather than a broad team rollout,
for three reasons:

1. **I already understand the cost model.** Our Claude Code usage is
   metered in ACUs (`ACU = (effective_input + effective_output) ×
   model_weight`), with output tokens weighted several times heavier than
   input and a 3-5x spread in cost between model tiers (Haiku vs. Sonnet vs.
   Opus). I'm building my usage habits around that math from day one, not
   retrofitting them after an overage alert.
2. **My workflow is naturally low-token by design, not by luck.** The bulk
   of my day-to-day data (Cohesity Helios API responses across our
   [10-50]-cluster environment) is large and raw. My plan is to never hand
   that to Claude directly — existing PowerShell/Dynatrace scripts already
   filter and format it into small digests (a few KB, not hundreds), and
   Claude only ever reads the digest. See the attached
   `cohesity-claude-integration-plan.md` for the full pipeline design.
3. **You'll be able to verify this, not just take my word for it.** Our ACU
   system alerts you directly if my usage crosses the 5-hour or weekly
   threshold. That's an existing, independent check — I'm not asking you to
   trust a self-reported number.

## The efficiency argument, with the actual math

Two ways to do the same task — say, turning today's backup-failure data
into a management summary — land very differently in ACU terms:

| Approach | Input | Model | Output | Rough ACU |
|---|---|---|---|---|
| Feed Claude the raw Helios API response, ask for a report | ~50,000 tokens, uncached | Opus | ~800 tokens, verbose | ~90,000 |
| Feed Claude a pre-filtered digest, ask for a short flagged summary | ~800 tokens, cached | Haiku | ~150 tokens, terse | ~270 |

*(These are illustrative placeholder token counts to show the mechanism —
not a measured result. I'll replace this table with real numbers from the
pilot's actual ACU dashboard.)*

The gap isn't about one approach being "smarter" — it's three compounding
choices, all self-imposed:

- **Never send raw API/log data** — always a pre-filtered digest.
- **Default to the smallest capable model** for routine work (Haiku is
  weighted ~3x cheaper than Sonnet, ~5x cheaper than Opus, for identical
  token counts) — reserving larger models for the occasional task that
  actually needs deeper reasoning.
- **Keep output short and structured** — bullet points and tables instead
  of prose, since output tokens are weighted several times heavier than
  input in our cost model, independent of model choice.

## What I'll actually use it for (pilot scope)

Ranked by how ready each is to start, and roughly how often it'd run:

1. **CR write-ups** (ServiceNow) — my single biggest time sink today.
   Drafting change descriptions, risk sections, and rollback plans from
   bullet notes. Ad hoc, per-CR — no pipeline needed, lowest-risk starting
   point.
2. **Ad hoc troubleshooting** — scripts, API errors, Cohesity/Dynatrace
   behavior. Occasional, interactive, no setup required.
3. **Backup-failure management reporting** — narrating an already-filtered
   digest into the daily email / weekly deck / dashboard commentary we
   already produce, plus flagging repeat or new failures. Scheduled, small,
   scripted (non-interactive) calls — the most predictable cost of the
   group.
4. **Maintenance & patching** — runbook drafting, patch-note
   summarization. Periodic.
5. **Incidents** — timeline/RCA drafting from a digest (not raw logs), once
   I've defined that digest format.
6. Confluence/Jira drafting, opportunistically.

I'm deliberately *not* starting with the highest-volume or highest-risk item
first — CR write-ups and troubleshooting are both low-cost per use and
immediately useful, which is the right shape for a pilot.

## What success looks like

- My ACU usage stays a small, predictable fraction of the [3,200,000/window]
  budget for routine work, with clear, expected spikes only for the
  occasional deeper investigation.
- No threshold alerts during the pilot window.
- [Add: qualitative or quantitative feedback on CR write-up turnaround,
  report clarity, etc. — to be filled in from actual pilot experience, not
  invented now.]

## What I'm asking for

- Individual Claude Code login (so usage is attributable to me, not pooled).
- A [2-4 week] pilot window scoped to the items above.
- A checkpoint at the end where we look at my actual ACU numbers together
  and decide whether to extend the scope to the rest of the backup admin
  team.
