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

Use one-cluster mode only for standalone testing or when the incident workflow output specifically provides the retry command.

```powershell
cd X:\PowerShell\Cohesity_API_Scripts\backup_failures
.\Cohesity_Backup_Failure_INC_Status_Update.ps1 -ClusterName "YOUR_CLUSTER_NAME"
```

## Optional grid view

For local review, the entry script can open the generated CSV files in PowerShell Grid View:

```powershell
cd X:\PowerShell\Cohesity_API_Scripts\backup_failures
.\Cohesity_Backup_Failure_INC_Status_Update.ps1 -ShowGrid
```

This opens filterable grid windows for:

```text
current_failures.csv
incident_lifecycle.csv
cleared_by_success.csv
```

`-ShowGrid` is optional and requires a PowerShell session where `Out-GridView` is available.

## Incomplete collection handling

If a cluster, environment, protection-group, or runs lookup times out, the run is marked incomplete in `worknotes_summary.txt`.

The note will show:

```text
Cohesity API Collection Status: Incomplete - <reason/count>
```

It will also show:

```text
Incomplete Collection:
- <warning detail>

Retry Failed Collection Scope:
<exact command to run>
```

Use the command printed in `worknotes_summary.txt`. After the rerun completes, use the refreshed `worknotes_summary.txt` and `incident_lifecycle.csv` for the incident update.

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
| `worknotes_summary.txt` | Main incident update. Contains count source, clear tally checks, incomplete collection details, and retry command when needed. |
| `closing_summary.txt` | Closure/handoff summary with the same count/tally model. |
| `state.json` | Script memory. Keeps failure state, failed run keys, cleared items, and warnings. |

## Sort order

The entry script normalizes the final text and CSV outputs into an operator-friendly order.

| Output | Sort order |
|---|---|
| `current_failures.csv` | Cluster ascending, then status priority, then `LastFailedET` descending, then Environment, ProtectionGroup, ObjectName. |
| `cleared_by_success.csv` | Cluster ascending, then `ClearedET` descending, then Environment, ProtectionGroup, ObjectName. |
| `incident_lifecycle.csv` | Cluster ascending, then status priority, then latest activity descending. Latest activity uses `ClearedET` when present, otherwise `LastSeenET`, otherwise `LastFailedET`. |
| `worknotes_summary.txt` | Summary-only. It points to the sorted CSV files for row-level detail. |
| `closing_summary.txt` | Summary-only. It points to the sorted CSV files for row-level detail. |

Status priority for active rows:

```text
NewlyFailedThisCheck
ReFailedAfterClear
OlderStillFailing / CurrentStillFailing
CarriedForwardStillFailing
RunningAtLatestCheck
CancelledAfterFailure
UnknownNeedsReview
```

This keeps each cluster together and keeps the most actionable statuses ahead of lower-confidence/unverified statuses.

## Worknotes summary format

`worknotes_summary.txt` is intentionally concise and count-focused.

It contains:

- Cohesity API Collection Status: Complete or Incomplete with reason/count.
- Count source explanation.
- Summary counts.
- Parent/child active failure breakdown.
- Tally checks.
- Incomplete collection details.
- Retry Failed Collection Scope command when needed.
- File attachment/update list.

It does not contain:

- Full lifecycle dump.
- Active failure row dump.
- Separate consecutive/repeated failure section.
- Failure error message on cleared-success rows.
- Local retention/output cleanup actions unless a cleanup issue is detected.
- `... more rows in CSV` truncation lines.

## Count model

The worknotes counts are based on CSV row counts.

```text
Active / unresolved failures = row count in current_failures.csv
Cleared by later successful backup = row count in cleared_by_success.csv
Total lifecycle rows tracked = row count in incident_lifecycle.csv
```

The active/unresolved child statuses must add up to the active/unresolved parent total:

```text
Newly failed this check
+ Older/current still failing
+ Carried forward still failing
+ Re-failed after earlier clear
+ Running / awaiting completion
+ Cancelled after failure
+ Needs review / not verified
= Active / unresolved failures
```

The lifecycle tally should normally be:

```text
Active / unresolved failures + Cleared by later successful backup = Total lifecycle rows tracked
```

If this does not match, the worknotes summary prints a `requires review` tally line and the team should use `incident_lifecycle.csv` for row-level validation.

## Operator wording for UnknownNeedsReview

Internal CSV status:

```text
UnknownNeedsReview
```

Worknotes wording:

```text
Needs review / not verified
```

Meaning:

```text
The script could not verify whether the item cleared. Treat it as unresolved until a later successful backup is verified.
```

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

1. Follow the `Retry Failed Collection Scope` command shown in `worknotes_summary.txt`.
2. After the rerun, update the incident using the refreshed `worknotes_summary.txt` and `incident_lifecycle.csv`.

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
- Retention/output cleanup messages are not shown in `worknotes_summary.txt` unless a local cleanup issue is detected.

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
