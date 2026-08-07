# Architecture Summary — Cohesity Intelligent Automation
**Status:** Design phase. Nothing built yet. This is the agreed foundation.  
**Last updated:** 2026-08-06

---

## What This Is

An intelligent, read-only operations platform for Cohesity backup environments.  
Claude Code is the intelligence layer. It does not replace existing tools — it connects them.

---

## Agreed Decisions

### Trigger Model
Two entry points. Both are valid. Neither replaces the other.

| Entry point | Use for | Output |
|---|---|---|
| Microsoft Teams (@Claude) | Ad-hoc queries, DBA/App team self-service | Teams thread reply + Jira audit entry |
| Jira ticket (label: `claude-*`) | Scheduled audits, Dynatrace alert triage | Jira comment + Teams notification |

Dynatrace keeps running. It continues detecting alerts. Its output changes from SNOW incident to Jira ticket. SNOW is not integrated and stays out of scope until separately approved.

### Runtime Cluster Discovery
`get_clusters()` runs first on every execution. No hardcoded cluster list anywhere.  
If `get_clusters()` fails → abort immediately. Do not proceed with cached data.

### Coverage Policy (non-negotiable)
- 100% cluster coverage required before posting any report
- If any cluster times out → retry 3x (30s / 60s / 120s intervals)
- If still unavailable after retries → HOLD the report, alert in Teams/Jira with cluster name
- Partial cluster data = same as unavailable. Never post a report with a silent gap.
- Every Jira comment must state: `N of N clusters checked. Window: X to Y.`

### Hallucination Guardrails
- Claude reports only what the API returned. No inference without evidence.
- Every finding references: cluster ID, source name, API field, timestamp
- Missing data = `NOT_FOUND`. Never filled in, never assumed.
- Structured output first (typed JSON), then formatted to Jira/Teams
- Claude never closes a Jira ticket. Posts findings + label `claude-findings-ready`. Human approves.

### Credentials
- Viewer service account only. One API key. No write access to Cohesity.
- API key in environment variable or vault. Never in files, CLAUDE.md, or project docs.
- All Cohesity API calls are GET-only. No exceptions.

### ServiceNow Boundary
SNOW is out of scope. Claude does not read from or write to SNOW in this project.  
SNOW integration is a separate project pending approval.

---

## Stack

```
Teams (@Claude)  ──┐
Jira (label)     ──┤──► Claude Code (claude -p, headless)
Dynatrace alert  ──┘         │
                              ├── Cohesity MCP (read-only, Helios)
                              │     GET https://helios.cohesity.com
                              │     accessClusterId header per cluster
                              │     Viewer API key
                              │
                              ├── Script Bridge (large clusters only)
                              │     PowerShell data reducers
                              │     Called by MCP, never by Claude directly
                              │
                              └── Jira MCP (read + comment)
                                    Posts findings. Never closes.
```

---

## Context Management

**Small queries (single cluster, single source):** MCP direct call.  
**Large clusters (>500 sources or >10k daily runs):** MCP calls script bridge. Script reduces data before Claude sees it.  
**Multi-cluster reports (23 clusters):** Subagents. One per cluster, parallel. Parent sees only summaries.  
Subagent batching: max 5-8 concurrent. Not all 23 at once. Avoids rate limits and cost spikes.

---

## Planned Use Cases (in build order)

1. **User & Group Review** — small, monthly, no subagents. Demo-ready now with existing PS script.
2. **Backup Failures** — 23 clusters, subagents + script bridge for large clusters.
3. **Alert Triage** — Dynatrace → Jira → Claude → initial triage findings.
4. **DBA/App Self-Service** — Teams as front door. Single DB or bulk DR check.

---

## Open Topics (separate chats)

- `claude/teams-integration-design.md` — Microsoft Teams connector approach (pending)
- `claude/mcp-data-contracts.md` — MCP tool schemas and error types (pending)
- `claude/subagent-batching-design.md` — Cluster batching strategy (pending)

---

## Source Scripts (personal GitHub, read-only reference)

Repo: `rupertrosario/my-redirect-page` branch: `Cohesity_Automations`

| Script | Role in new architecture |
|---|---|
| `backup_failures/Get-CohesityBackupFailureWindowConsolidator.ps1` (V6) | Large-cluster data reducer for failures |
| `inventory/Get-CohesityProtectionInventory.ps1` | Inventory data reducer |
| `Cohesity_SQL_Inventory/Get-CohesitySQLInventory.ps1` | DBA self-service data reducer |
| `Interface/01_get_alerts.js` + `02_validate_interfaces.js` | Alert type codes, Helios alert API pattern |
| `cohesity-dashboard-collector/modules/Get-HeliosData.ps1` | Helios auth and cluster map pattern |
| `Cohesity_AD/Get-CohesityADConfiguration.ps1` | AD config pattern |
| `Cohesity_SSO/Cohesity_Helios_SSO_Configuration_Inventory.ps1` | SSO/identity pattern |
