# Cohesity Intelligent Automation — Architecture Plan
**Author:** R R (Backup Architect)  
**Date:** 2026-08-06  
**Version:** 2.0 (updated from repo analysis)

---

## What We Know From The Repo

After reading the existing scripts at `rupertrosario/my-redirect-page` (branch: `Cohesity_Automations`), the architecture is grounded in real patterns:

- **All API calls go through Helios** (`https://helios.cohesity.com`), with per-cluster routing via `accessClusterId` header. Not individual cluster IPs.
- **Auth is API key** passed as `apiKey` header. Existing scripts use AES-encrypted key on disk. New viewer service account will use a plain API key stored securely.
- **Scripts are already GET-only** — this is already enforced in every existing script.
- **Dynatrace already handles alert detection** — interface alerts (types 1105, 13023) are already detected and processed as Dynatrace JS workflows.
- **The DTSK workflow already has Jira closure ideas** — `09_jira_closure_and_snow_idea.md` documents exactly this transition.
- **V6 backup failure consolidator is mature** — it already handles 23-cluster lifecycle logic, state reconciliation, and condensed CSV output.

---

## The Existing Stack (What's Already Running)

```
Dynatrace Workflows (JS)          PowerShell Scripts
─────────────────────────         ──────────────────────────────
Interface alert detection         Get-CohesityBackupFailureWindowConsolidator.ps1 (V6)
  → alert types 1105, 13023       Get-CohesityProtectionInventory.ps1
  → validate interfaces           Get-CohesitySQLInventory.ps1
  → creates SNOW incident         Get-PhysicalPGInventory.ps1
                                  Get-CohesityADConfiguration.ps1
DTSK backup status check          Get-CohesitySecurityConfiguration.ps1
  → SNOW search → Helios API      policy_summary_alignment/poli.ps1
  → email report only (now)

All hitting: https://helios.cohesity.com
Auth: apiKey header + accessClusterId header per cluster
```

**The gap:** Everything outputs to email, CSV, or SNOW. Nothing goes to Jira. Nothing has Claude-level analysis. No closed-loop automation.

---

## The New Stack (What We're Building)

```
                        ┌─────────────────────────────────┐
                        │         ORG GIT REPO             │
                        │       cohesity-audit/            │
                        └────────────────┬────────────────┘
                                         │
              ┌──────────────────────────┼──────────────────────┐
              │                          │                       │
              ▼                          ▼                       ▼
   ┌─────────────────┐       ┌───────────────────┐   ┌──────────────────┐
   │  Cohesity MCP   │       │    Jira MCP        │   │ Script Bridge    │
   │  (read-only)    │       │  (read + write)    │   │ (data reducers)  │
   │  viewer API key │       │  tickets, comments │   │ V6 consolidator  │
   │  Helios only    │       │  labels, close     │   │ SQL inventory    │
   └────────┬────────┘       └─────────┬──────────┘   └────────┬─────────┘
            │                          │                        │
            └──────────────┬───────────┘                        │
                           ▼                                    │
                  ┌──────────────────┐                          │
                  │   Claude Code    │◄─────────────────────────┘
                  │  (intelligence)  │
                  └──────────────────┘
                           ▲
              ┌────────────┴───────────┐
              │                        │
   ┌──────────────────┐    ┌──────────────────────┐
   │ Dynatrace        │    │ Jira (all teams)      │
   │ (keeps running)  │    │ Ops / DBA / App teams │
   │ alert detection  │    │ raise tickets here    │
   └──────────────────┘    └──────────────────────┘
```

**The key change:** Dynatrace doesn't get replaced. It gets re-wired to output to Jira instead of SNOW. Claude picks up from Jira, does the intelligence work, posts findings back.

---

## Git Strategy

```
Personal GitHub (read-only reference)         Org GitHub (active project)
rupertrosario/my-redirect-page                cohesity-audit/ (new repo)
branch: Cohesity_Automations                  ─────────────────────────────
─────────────────────────────                 MCP server code
Existing scripts (reference)                  Audit definitions
Dynatrace JS workflows (reference)            CLAUDE.md
Interface alert scripts                       Slash commands
V6 failure consolidator                       Docs
SQL inventory                                 Copied/adapted scripts
                 │                                      ▲
                 └─────── read, adapt, copy ────────────┘
```

**Rules:**
- Never write to personal GitHub
- Personal GitHub = source of truth for existing script logic
- Org GitHub = where everything new lives and runs
- When testing at home: clone from org repo, run locally
- When ready: push to org repo, execution picks up automatically

---

## Repo Structure (Org GitHub)

```
cohesity-audit/
├── CLAUDE.md                        ← routing rules, cluster list, context
├── README.md
│
├── mcp/
│   ├── cohesity/
│   │   ├── server.py                ← MCP server, Helios base URL
│   │   ├── auth.py                  ← API key from env var, not encrypted file
│   │   └── tools/
│   │       ├── clusters.py          ← GET /v2/mcm/cluster-mgmt/info
│   │       ├── protection_groups.py ← GET /v2/data-protect/protection-groups
│   │       ├── sources.py           ← GET /v2/data-protect/sources
│   │       ├── jobs.py              ← GET /v2/data-protect/protection-groups/{id}/runs
│   │       ├── alerts.py            ← GET /v2/alerts (types: 1077,1105,13023)
│   │       ├── users.py             ← GET /v1/users + /v1/groups
│   │       ├── policies.py          ← GET /v2/data-protect/policies
│   │       ├── storage.py           ← GET /v1/stats/storage
│   │       ├── objects.py           ← GET /v2/data-protect/objects/{id}
│   │       └── bridge.py            ← calls scripts for large-cluster jobs
│   │
│   └── jira/
│       ├── server.py
│       └── tools/
│           ├── tickets.py           ← read, create, comment, close
│           └── search.py            ← search by label/project
│
├── scripts/                         ← data reducers only, called by bridge.py
│   ├── Get-BackupFailures.ps1       ← adapted from V6 consolidator
│   ├── Get-ProtectionInventory.ps1  ← adapted from existing inventory script
│   └── Get-SQLInventory.ps1         ← adapted from SQL inventory script
│
├── audits/                          ← what to check, what to flag, output format
│   ├── user-group-review.md
│   ├── backup-failures.md
│   ├── alert-triage.md
│   ├── paused-pg.md
│   ├── policy-compliance.md
│   ├── sql-db-selfservice.md
│   └── inventory.md
│
├── commands/                        ← Claude Code slash commands
│   ├── failed-backups.md
│   ├── user-review.md
│   ├── alert-triage.md
│   ├── db-status.md
│   └── inventory.md
│
└── docs/
    ├── architecture.md
    ├── helios-api-patterns.md       ← real endpoints from existing scripts
    ├── cluster-inventory.md         ← 23 clusters, which 5 are large
    └── runbooks/
```

---

## Cohesity MCP — Real API Endpoints

All calls to `https://helios.cohesity.com`. Per-cluster routing via `accessClusterId` header.

| MCP Tool | Helios Endpoint | Already Used In |
|---|---|---|
| `get_clusters` | `GET /v2/mcm/cluster-mgmt/info` | 01_get_alerts.js, dashboard collector |
| `get_protection_groups` | `GET /v2/data-protect/protection-groups?environments=kSQL&isActive=true` | SQL inventory |
| `get_pg_runs` | `GET /v2/data-protect/protection-groups/{id}/runs?numRuns=N&includeObjectDetails=true` | SQL inventory, V6 consolidator |
| `get_policies` | `GET /v2/data-protect/policies` | inventory, SQL inventory |
| `get_alerts` | `GET /v2/alerts?alertTypes=1077,1105,13023&alertStates=kOpen` | 01_get_alerts.js |
| `get_sources` | `GET /v2/data-protect/sources` | inventory |
| `get_object` | `GET /v2/data-protect/objects/{id}` | SQL inventory |
| `get_users` | `GET /v1/users` | user/group Dynatrace workflow |
| `get_groups` | `GET /v1/groups` | user/group Dynatrace workflow |
| `get_storage` | `GET /v1/stats/storage` | dashboard collector |
| `bridge_script` | internal → PowerShell | large clusters only |

Auth header: `apiKey: <viewer-service-account-key>`  
Per-cluster: `accessClusterId: <clusterId>` (cluster IDs from `get_clusters`)

---

## Context Management — Real Numbers

**Problem:**
- 23 clusters, 5 large
- V6 consolidator already handles this for failures — it runs per-cluster, produces condensed CSVs
- The question is: how does Claude interact with this data without filling context?

**Three-layer approach, using what we know:**

**Layer 1 — MCP filtering (18 smaller clusters)**
Direct Helios API call with filters. Already proven in existing JS scripts.
```python
# mirrors existing JS pattern
get_pg_runs(cluster_id, num_runs=5, status_filter="kFailed")
# returns condensed list, not raw 50k rows
```

**Layer 2 — Script bridge (5 large clusters)**
`bridge.py` calls adapted V6 consolidator. Script already knows how to:
- Take `NumRuns` and `BaselineNumRuns` parameters
- Produce `current_failures.csv` (only active failures, not all runs)
- Return a condensed state, not raw API dumps

Claude sees: `{cluster, failed_count, critical_list, recurring_objects}`  
Claude never sees: 50,000 raw run records

**Layer 3 — Subagents (all 23 clusters in parallel)**
```
Parent Agent
  Reads: Jira ticket, cluster list from CLAUDE.md
  │
  ├── Task → Subagent: cluster-01 through cluster-18  [MCP direct, 18 parallel]
  └── Task → Subagent: cluster-19 through cluster-23  [MCP + script bridge, 5 parallel]

Each subagent returns:
{
  cluster: "PROD-CL-01",
  failed: 47,
  critical: 3,          ← same source failing 3+ consecutive days
  recurring: ["SourceA (5d)", "SourceB (3d)"],
  new_this_window: 12,
  cleared_this_window: 8
}

Parent context: 23 small summaries → aggregate → post to Jira
```

**Time:** All 23 run in parallel. Total time = slowest single cluster.  
**Context:** Parent never holds raw data. Each subagent context is small and disposable.

---

## Use Case 1 — User & Group Review (Small, Monthly)

**Source scripts:** Cohesity User, Groups and Roles Report (existing Dynatrace workflow, weekly)  
**New:** Instead of email → Jira ticket with findings, Claude-level analysis

**Trigger:** Jira automation creates ticket 1st of each month, label `claude-audit-users`

**API calls (no subagents needed):**
```
GET /v1/users                    → all users, roles, last_login
GET /v1/groups                   → groups, membership
GET /v1/activeDirectory          → AD integration status
```

**What Claude checks:**
- Users inactive > 60 days (no last_login or last_login > 60 days ago)
- Service accounts with admin or super_admin role (flag for review)
- Users with no group membership (orphaned users)
- Groups with 0 members (orphaned groups)  
- Users added in last 30 days (new — flag for review)
- Users with roles that don't match their AD group assignment

**Output to Jira:**
```
| User | Role | Last Login | Groups | Status | Action Required |
|------|------|------------|--------|--------|-----------------|
| svc-backup | Admin | Never | None | ⚠️ Flag | Review admin role |
| john.doe | Viewer | 94 days ago | BackupOps | ⚠️ Inactive | Confirm still needed |
```

**Closes ticket automatically** unless critical flags are found.  
**If critical flags:** adds `needs-review` label, assigns to ops team, leaves open.

---

## Use Case 2 — Backup Failures Report (Large, 23 Clusters)

**Source scripts:** `Get-CohesityBackupFailureWindowConsolidator.ps1` (V6) — already battle-tested  
**New:** Subagent architecture, Jira output, Claude-level pattern analysis

**Trigger:** Jira automation creates ticket daily (or 4-hourly for production), label `claude-audit-failures`

**Window logic:** Mirrors existing V6 — 18:00 ET to next-day 18:00 ET window

**Flow:**
```
Jira ticket: "Backup Failure Report - 2026-08-06 18:00 ET window"
    │
    Parent Agent
    │   reads: ticket, cluster list from CLAUDE.md
    │   identifies: 5 large clusters (in CLAUDE.md)
    │
    ├── 18 small clusters → Subagents → MCP: get_pg_runs(status=failed, numRuns=5)
    │                                         → returns condensed summary
    │
    └── 5 large clusters → Subagents → MCP bridge → Get-BackupFailures.ps1
                                                     (adapted V6 logic)
                                                   → returns condensed summary

    Parent collects 23 summaries:
    ├── Total active failures across all clusters
    ├── Critical: same object failing 3+ consecutive days
    ├── New this window: appeared for first time
    ├── Cleared this window: previously failing, now success
    ├── Recurring pattern: same error type across multiple clusters
    └── RemoteAdapter: excluded (same as V6)

    Posts to Jira:
    ├── Executive summary (4 lines: total, critical, new, cleared)
    ├── Cluster breakdown table
    ├── Critical objects list (needs immediate action)
    └── Pattern analysis (systemic issues worth escalating)

    Closes ticket if no critical items.
    Flags and assigns if critical items found.
```

**Object identity rule** (from V6): `Cluster + ProtectionGroup + Environment + ObjectIdentity`  
Not `RunType` — carries forward from existing V6 logic.

---

## Use Case 3 — Alert Triage (Reactive, Interface Down)

**Source scripts:** `Interface/01_get_alerts.js` + `Interface/02_validate_interfaces.js`  
**Existing flow:** Dynatrace detects → creates SNOW incident  
**New flow:** Dynatrace detects → creates Jira ticket → Claude triages

**Alert types being handled:**
- `1077` — interface-related
- `1105` — interface down
- `13023` — interface down (alternate code)

**New Dynatrace wiring:**
```
Dynatrace workflow (keep existing alert detection JS)
    └── Change output: instead of SNOW incident → create Jira ticket
        Label: claude-alert
        Title: "Interface Alert - {ClusterName} - {NodeId} - {IP}"
        Body: alert details from existing JS output
```

**Claude triage flow:**
```
Jira ticket: "Interface Alert - PROD-CL-07 - Node-03 - 10.1.2.45"
    │
    Claude reads ticket
    │
    ├── MCP: get_clusters() → check PROD-CL-07 health, node count, quorum
    ├── MCP: get_alerts(cluster=PROD-CL-07, types=1105,13023) → full alert detail
    ├── MCP: get_pg_runs(cluster=PROD-CL-07, last_6h) → any job failures since alert
    ├── MCP: get_sources(cluster=PROD-CL-07, node=node-03) → sources on affected node
    │
    Claude posts initial triage to Jira:
    ├── Alert summary: interface down since {time}, node {id}, IP {ip}
    ├── Cluster health: {N} nodes, quorum {ok/at-risk}
    ├── Impact: {N} sources on this node, {M} jobs failed/at-risk
    ├── Last successful backup for critical sources: {list}
    ├── Hypothesis: {switch port issue / cable / NIC}
    └── Recommended next step: check switch port for {IP}, escalate to infra team

    Adds label: triage-complete, initial-assessment-posted
    Does NOT close — human action required for infrastructure issues
```

**Other alert types (future):**
| Alert | Claude action |
|---|---|
| Replication failure | Check source job + target cluster health |
| Storage near quota | Identify largest consumers, flag top 5 PGs |
| Node down | Check cluster quorum, impacted sources |
| Job failure (single) | Check if recurring, identify error pattern |
| Certificate expiry | Report cert, expiry date, affected services |

---

## Use Case 4 — DBA & App Team Self-Service

**Source scripts:** `Get-CohesitySQLInventory.ps1` (existing, DB-level detail)  
**New:** Jira as the front door, no direct Cohesity access for other teams

**Who uses this:**
- DBA team: backup status per Oracle/SQL database
- App team: backup status per application server
- Both teams: DR readiness checks before maintenance windows

**Jira project:** `BACKUPQ` (separate from ops `BACKUPOPS`)  
**Label:** `backup-query`

**Flow for single DB:**
```
Jira ticket (raised by DBA): "Backup Status - prod-oracle-01"
    │
    Claude reads ticket, extracts source name
    │
    ├── MCP: get_clusters() → find which cluster has this source
    ├── MCP: get_sources(name="prod-oracle-01") → source ID, PG assignment
    ├── MCP: get_pg_runs(source_id, last_7_days) → backup history
    ├── MCP: get_policies(pg_id) → policy name, RPO, retention
    │
    Claude posts:
    ├── Source: prod-oracle-01
    ├── Cluster: PROD-CL-05
    ├── Protection Group: PG-Oracle-Prod
    ├── Policy: Gold-30day | RPO: 24h | Retention: 30 days
    ├── Last Successful Backup: 2026-08-05 23:14 IST ✅
    ├── Last 7 Days: 7/7 successful
    ├── Next Scheduled: 2026-08-06 23:00 IST
    └── Status: Healthy — DR readiness confirmed
```

**Flow for DR readiness (bulk check):**
```
Jira ticket: "DR Check - APAC Maintenance Window - 15 servers"
    Body: [list of server names]
    │
    Parent Agent reads ticket, extracts server list
    │
    ├── Spawns one subagent per server (parallel)
    │   Each subagent: finds source → gets last backup → returns status
    │
    Parent aggregates:
    ├── Ready: 12/15 servers (last backup < 24h, healthy)
    ├── At Risk: 2/15 (last backup > 48h)
    └── Not Protected: 1/15 (no PG found)

    Posts DR readiness table to Jira
    Flags at-risk servers for ops team action
```

---

## Jira Setup

**Projects:**
- `BACKUPOPS` — ops team: audits, failures, alerts (Claude auto-manages)
- `BACKUPQ` — all other teams: self-service queries

**Labels Claude watches:**
| Label | Audit | Who Creates Ticket |
|---|---|---|
| `claude-audit-users` | User & Group Review | Jira automation (monthly) |
| `claude-audit-failures` | Backup Failures | Jira automation (daily/4-hourly) |
| `claude-alert` | Alert Triage | Dynatrace workflow (real-time) |
| `claude-audit-paused` | Paused PGs | Jira automation (weekly) |
| `claude-audit-policy` | Policy Compliance | Jira automation (monthly) |
| `backup-query` | Self-Service | DBA / App team (on-demand) |

---

## CLAUDE.md Routing (Key Section)

```markdown
## Helios API
Base URL: https://helios.cohesity.com
Auth header: apiKey (from env: COHESITY_API_KEY)
Per-cluster header: accessClusterId

## Large Clusters (use script bridge)
LARGE_CLUSTERS: [cluster-id-1, cluster-id-2, cluster-id-3, cluster-id-4, cluster-id-5]
Threshold: > 500 sources OR > 10,000 daily runs

## Routing Rules
| Task | Method |
|------|--------|
| Single source lookup | MCP direct |
| Single cluster health | MCP direct |
| User/group review | MCP direct (no subagents) |
| Alert triage | MCP direct |
| Failures < 5 clusters | MCP direct |
| Failures all 23 clusters | Subagents per cluster |
| Large cluster failures | Subagents + script bridge |
| DR readiness > 5 sources | Subagents per source |

## Jira Label → Audit Definition
claude-audit-users    → audits/user-group-review.md
claude-audit-failures → audits/backup-failures.md
claude-alert          → audits/alert-triage.md
claude-audit-paused   → audits/paused-pg.md
claude-audit-policy   → audits/policy-compliance.md
backup-query          → audits/sql-db-selfservice.md
```

---

## Build Phases

### Phase 1 — MCP Foundation (Week 1-2)
- Create org repo `cohesity-audit`
- Build Cohesity MCP: auth, cluster list, protection groups, job runs, alerts
- Test against one cluster using viewer API key
- Write CLAUDE.md with real cluster IDs and routing rules

### Phase 2 — Small Use Case (Week 2-3)
- Write `audits/user-group-review.md`
- Configure Jira MCP (read + comment + close)
- Create slash command `/user-review`
- End-to-end test: Jira → Claude → Helios → findings → Jira close

### Phase 3 — Failure Report (Week 3-5)
- Adapt V6 consolidator into `scripts/Get-BackupFailures.ps1`
- Implement `bridge.py` for large clusters
- Write `audits/backup-failures.md`
- Implement subagent pattern in Claude Code
- Test across all 23 clusters
- Create slash command `/failed-backups`

### Phase 4 — Alert Triage (Week 5-6)
- Re-wire Dynatrace interface workflow output to Jira (change SNOW → Jira ticket creation)
- Write `audits/alert-triage.md` for each alert type
- Test with interface-down alert type end-to-end

### Phase 5 — DBA / App Self-Service (Week 6-7)
- Adapt SQL inventory script for `scripts/Get-SQLInventory.ps1`
- Write `audits/sql-db-selfservice.md`
- Create `BACKUPQ` Jira project
- Onboard DBA team with instructions
- Test single DB query and bulk DR check

### Phase 6 — Full Automation (Week 7-8)
- Jira automation rules for all scheduled audits
- Monitor Claude picking up all label types
- End-to-end test with no human in loop
- Document in docs/ and runbooks/

---

## Presentation — 3 Points

**Point 1 — What we're building**
A read-only intelligence layer on top of Cohesity Helios. One viewer service account, one API key, zero writes. Jira is the front door for every team — operations, DBA, app teams. Claude is the intelligence layer. Everything runs from Git.

**Point 2 — What it automates**
11 recurring audit and reporting tasks that currently require manual effort: backup failures across 23 clusters, user and group review, paused protection groups, policy compliance, alert triage, DR readiness checks, and on-demand DB backup queries for DBA and app teams. Dynatrace keeps detecting alerts — it just stops writing to SNOW and starts writing to Jira instead.

**Point 3 — The architecture is safe**
Read-only always. No write access to Cohesity. Human in the loop for infrastructure actions. Claude posts findings and flags; humans decide what to do. Audit trail in Jira. Everything versioned in Git.

---

## References From Repo

| File | Purpose in New Architecture |
|---|---|
| `backup_failures/Get-CohesityBackupFailureWindowConsolidator.ps1` | Adapted as large-cluster data reducer |
| `Interface/01_get_alerts.js` | Alert type codes (1077, 1105, 13023) and Helios alert API pattern |
| `Interface/02_validate_interfaces.js` | Interface validation logic reference |
| `inventory/Get-CohesityProtectionInventory.ps1` | Inventory data reducer, Helios API patterns |
| `Cohesity_SQL_Inventory/Get-CohesitySQLInventory.ps1` | DBA self-service data reducer |
| `cohesity-dashboard-collector/modules/Get-HeliosData.ps1` | Helios auth and cluster map pattern |
| `dtsk_backup_status/09_jira_closure_and_snow_idea.md` | Confirms Jira transition is already planned |
| `workflow_catalog/Cohesity_Workflow_Names_and_Descriptions.md` | Full list of existing Dynatrace workflows |
