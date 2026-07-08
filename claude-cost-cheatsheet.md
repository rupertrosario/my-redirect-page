# Claude Code cost cheat sheet (backup admin work)

## Dollar conversion

`$ = ACU × $0.000003`. Only the Claude call costs ACU — Dynatrace/PowerShell/Helios API calls are $0 in this metering.

| Approach | ACU/run | $/run | $/month (daily) | Runs before it trips your Sr Director's alert (3.2M ACU/window) |
|---|---|---|---|---|
| Digest → Haiku → short summary | ~800 | $0.0024 | ~$0.07 | ~4,000 |
| Raw JSON → Opus → verbose report | ~90,000 | $0.27 | ~$8.10 | ~35 |

Rule of thumb: **never send raw API JSON, keep output short, default to Haiku.** That's the whole cost strategy — output tokens cost 5x input, and Haiku is ~3x cheaper than Sonnet for the same tokens.

## Persona prompt (paste into Claude Code at work)

```
Backup admin, team of admins, Cohesity/Helios (10-50 clusters), Dynatrace +
PowerBI. Work: ServiceNow CR write-ups (biggest time sink), incidents,
maintenance/patching, script troubleshooting, Confluence, Jira.

Rules: default to Haiku unless a task needs real reasoning. Keep answers
short — bullets/tables, no restating my input, no filler. Never process raw
API dumps or full logs — ask me for a filtered digest first.
```

## Sr Director ask (one paragraph)

I want to pilot Claude Code on my individual login for CR write-ups,
troubleshooting, and backup-failure report narration. Cost model: digest-in,
Haiku, short output → ~$0.0024/run, ~$0.07/month for the daily report. Usage
is metered in ACUs and auto-alerts you if I cross the window threshold, so
this is checkable, not a promise. Review after 2-4 weeks.
