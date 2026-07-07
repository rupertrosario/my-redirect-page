# Cohesity Backup Failure INC Status Update

## Script

Operational entry point:

```text
backup_failures/Cohesity_Backup_Failure_INC_Status_Update.ps1
```

Main implementation file retained in the same folder:

```text
backup_failures/Get-CohesityBackupFailureWindowConsolidator.ps1
```

## Purpose

Incident lifecycle tracker for Cohesity backup failures.

It tracks current/latest uncleared failures observed during the incident window, detects new/old/carry-forward failures, and marks a failure cleared only when a later successful backup is verified.

This is an incident operations workflow. It is not an audit-grade collector for every historical backup event.

## Important operator rule

Use only the new incident lifecycle script.

Do not use the legacy backup-failure script as a fallback for this incident workflow.
Do not import legacy backup-failure CSV files into this workflow.
Do not manually merge legacy output into the incident lifecycle files.

## Run all clusters

This is the standard run mode.

```powershell
cd X:\PowerShell\Cohesity_API_Scripts\backup_failures
.\Cohesity_Backup_Failure_INC_Status_Update.ps1
```

## Run one cluster

Use one-cluster mode only for standalone testing unless targeted partial-update mode has been explicitly added and confirmed.

```powershell
cd X:\PowerShell\Cohesity_API_Scripts\backup_failures
.\Cohesity_Backup_Failure_INC_Status_Update.ps1 -ClusterName "YOUR_CLUSTER_NAME"
```

Do not run a one-cluster scan against an existing all-cluster incident and assume the output is complete for the full estate.

## Timeout / incomplete collection handling

If `worknotes_summary.txt` says the report is incomplete, or if warnings show PG/cluster lookup timeout:

1. Do not close the incident based only on that run.
2. Do not treat missing objects from the warned cluster/protection group as cleared.
3. Review the `Warnings / Incomplete Collection` section in `worknotes_summary.txt`.
4. Re-run the new incident lifecycle script after the timeout condition is expected to clear.
5. Use only the refreshed files from the new incident lifecycle script.

Planned safe retry model:

```powershell
.\Cohesity_Backup_Failure_INC_Status_Update.ps1 -IncidentNumber "INCxxxxxxx" -ClusterName "FAILED_CLUSTER" -Environment "kOracle" -PartialUpdate
```

This targeted partial-update mode must preserve all existing incident rows outside the failed retry scope. Until that mode is implemented and confirmed, do not use one-cluster output as a full incident update for an existing all-cluster incident.

## Locked design summary

- One incident is locked to one compute window.
- Compute window is for incident ownership only.
- First run creates the incident baseline.
- Later runs compare the latest current failure scan against `state.json`.
- New object in current scan = new failure.
- Existing object still present = older/current still failing.
- Previously cleared object appearing again = re-failed.
- Previous unresolved object still active in a new window = carried forward.
- Old object missing from current scan is not automatically cleared.
- Only `Succeeded` or `SucceededWithWarning` clears a failure.
- `Running` does not clear.
- `Cancelled` / `Canceled` does not clear.
- Unknown, timeout, or incomplete API data does not clear.
- Consecutive count is based on unique failed backup runs, not script executions.
- Evaluation is limited to the latest 30 runs per protection group/run type.

## Output folder

```text
X:\PowerShell\Data\Cohesity\BackupFailureWindow\<INC_NUMBER>\
```

## Output files

```text
current_failures.csv
cleared_by_success.csv
incident_lifecycle.csv
worknotes_summary.txt
closing_summary.txt
state.json
```

### File purpose

| File | Purpose |
|---|---|
| `current_failures.csv` | Main action list. Active/unresolved failures for the team to work. |
| `cleared_by_success.csv` | Failures verified as cleared by a later successful backup. |
| `incident_lifecycle.csv` | Consolidated incident view with all tracked objects and current status. This is the best sortable operational detail file. |
| `worknotes_summary.txt` | Full audit-ready incident update. Upload/paste this into the incident. No row truncation. |
| `closing_summary.txt` | Closure/handoff summary. Includes active/unresolved failures and successful backup clearances. No row truncation. |
| `state.json` | Script memory. Keeps failure state, failed run keys, cleared items, and warnings. |

## Human-readable reporting rule

To avoid conflicting, partial, or audit-confusing reporting:

- There is one main human-readable incident update: `worknotes_summary.txt`.
- `worknotes.txt` and `summary.txt` are not retained by the entry script.
- `worknotes_summary.txt` includes all row details with no `... more rows in CSV` truncation lines.
- `worknotes_summary.txt` includes current failures, carried-forward failures, running/cancelled/unknown items, successful backup clearances, lifecycle rows, and warning/timeout details.
- `closing_summary.txt` is the separate closure/handoff note and also includes successful backup clearances.
- CSV files remain sortable evidence, but `worknotes_summary.txt` is complete enough to understand the incident update without opening the CSVs.

## Status values

```text
NewlyFailedThisCheck
OlderStillFailing
CarriedForwardStillFailing
ClearedByLaterSuccess
NewlyClearedThisCheck
ReFailedAfterClear
RunningAtLatestCheck
CancelledAfterFailure
UnknownNeedsReview
```

## Required incident attachments / updates

For each clean run:

1. Upload or paste `worknotes_summary.txt`.
2. Attach `incident_lifecycle.csv`.
3. Attach `current_failures.csv` if active failures remain.
4. Attach `cleared_by_success.csv` if rows exist.
5. Use `closing_summary.txt` only during closure or handoff.

For incomplete runs:

1. Upload/paste `worknotes_summary.txt` only if the incomplete warning is clearly visible.
2. Do not close the incident.
3. Do not claim all failures are cleared.
4. Re-run the new incident lifecycle script and attach the refreshed output.

## Previous incident closure summary

When a new compute window starts and the script needs a new incident number, it first refreshes the previous incident's:

```text
closing_summary.txt
```

This uses the previous incident state/files only. It does not perform a new Cohesity scan for the previous incident.

## Retention

Base folder:

```text
X:\PowerShell\Data\Cohesity\BackupFailureWindow
```

Retention behavior:

| Age | Action |
|---:|---|
| 0-14 days | Keep incident folders as normal folders. |
| 15-35 days | Zip incident folder to `Archive`, then delete original folder after zip succeeds. |
| Older than 35 days | Delete old zip files and any remaining old folders. |

Safety rules:

- Active incident folder is never zipped or deleted.
- Folder is deleted only after zip exists.
- Retention actions and warnings are written into `worknotes_summary.txt`.

## Authentication

Uses the existing encrypted API key method only:

```text
X:\PowerShell\Cohesity_API_Scripts\Common\ApiKeyAesHelper.ps1
X:\PowerShell\Cohesity_API_Scripts\Common\Secure\cohesity_apikey.enc
```

## Notes

- No Excel output.
- No ServiceNow writes.
- Cohesity API calls are GET-only.
- No legacy backup-failure CSV fallback in the operator process.
- No `Recovered` / `Recovery` wording is used for backup clearances.
- Scope line is written to `worknotes_summary.txt`: latest 30 runs per protection group/run type.
