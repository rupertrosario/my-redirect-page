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

It is not an audit-grade collector for every backup event. It tracks current/latest uncleared failures observed during the incident window, detects new/old/carry-forward failures, and marks a failure cleared only when a later successful backup is verified.

## Run one cluster

```powershell
cd X:\PowerShell\Cohesity_API_Scripts\backup_failures
.\Cohesity_Backup_Failure_INC_Status_Update.ps1 -ClusterName "YOUR_CLUSTER_NAME"
```

## Run all clusters

```powershell
cd X:\PowerShell\Cohesity_API_Scripts\backup_failures
.\Cohesity_Backup_Failure_INC_Status_Update.ps1
```

## Optional first baseline from existing failure CSV

Use this only when you already ran the existing failure-only script and want the first incident baseline from its latest combined CSV:

```powershell
.\Cohesity_Backup_Failure_INC_Status_Update.ps1 -UseLatestFailureCsv
```

Default legacy CSV location:

```text
X:\PowerShell\Data\Cohesity\BackupFailures\BackupFailures_AllEnvironments_*.csv
```

You can also provide an exact CSV:

```powershell
.\Cohesity_Backup_Failure_INC_Status_Update.ps1 -UseLatestFailureCsv -LegacyFailureCsvPath "X:\PowerShell\Data\Cohesity\BackupFailures\BackupFailures_AllEnvironments_YYYYMMDD_HHMMSS.csv"
```

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
- Unknown or incomplete API data does not clear.
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
worknotes.txt
summary.txt
closing_summary.txt
state.json
```

### File purpose

| File | Purpose |
|---|---|
| `current_failures.csv` | Main action list. Active/unresolved failures for the team to work. |
| `cleared_by_success.csv` | Failures verified as cleared by a later successful backup. |
| `incident_lifecycle.csv` | Consolidated incident view with all tracked objects and current status. |
| `worknotes.txt` | Full audit-ready incident update. Same detailed content as `summary.txt`. No partial updates. |
| `summary.txt` | Full audit-ready incident update. Same detailed content as `worknotes.txt`. No row truncation. |
| `closing_summary.txt` | Closure/handoff summary. Includes active/unresolved failures and successful backup clearances. No row truncation. |
| `state.json` | Script memory. Keeps failure state, failed run keys, cleared items, and warnings. |

## Human-readable reporting rule

To avoid conflicting, partial, or audit-confusing reporting:

- `worknotes.txt` and `summary.txt` intentionally contain the same full detailed incident update.
- Neither file is a short pointer/index note.
- Both files include all row details with no `... more rows in CSV` truncation lines.
- Both files include current failures, carried-forward failures, running/cancelled/unknown items, successful backup clearances, lifecycle rows, and warning/timeout details.
- `closing_summary.txt` is the detailed closure/handoff note and also includes successful backup clearances.
- CSV files remain sortable evidence, but the text note is complete enough to understand the incident update without opening the CSVs.

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
- Retention actions and warnings are written into `summary.txt` and `worknotes.txt`.

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
- No `Recovered` / `Recovery` wording is used for backup clearances.
- Scope line is written to worknotes and summary: latest 30 runs per protection group/run type.
