# Cohesity Backup Failure INC Status Update

## Script

Operational entry point:

```text
backup_failures/Cohesity_Backup_Failure_INC_Status_Update.ps1
```

Main collector retained in the same folder:

```text
backup_failures/Get-CohesityBackupFailureWindowConsolidator.ps1
```

Operators should run only the entry script.

## Purpose

Incident lifecycle tracker for Cohesity backup failures.

It tracks current/latest uncleared failures observed during the incident lifecycle, detects new/old/carry-forward failures, and marks a failure cleared only when a later successful backup is verified.

This is an incident operations workflow. It is not an audit-grade collector for every historical backup event.

## Run all clusters

Standard run mode:

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

## Optional lifecycle grid view

```powershell
cd X:\PowerShell\Cohesity_API_Scripts\backup_failures
.\Cohesity_Backup_Failure_INC_Status_Update.ps1 -ShowGrid
```

This opens only:

```text
incident_lifecycle.csv
```

Reason: `incident_lifecycle.csv` is the complete row-level evidence file. It contains active/unresolved rows, cleared rows, running/cancelled/needs-review rows, and carry-forward status.

`-ShowGrid` is optional and requires a PowerShell session where `Out-GridView` is available.

## Final operator-facing files

The entry script leaves only these operator-facing files for incident use:

```text
worknotes_summary.txt
incident_lifecycle.csv
closing_summary.txt
```

The script also keeps:

```text
state.json
```

`state.json` is script memory. Do not manually edit it and do not attach it to the incident.

Temporary collector files such as `current_failures.csv`, `cleared_by_success.csv`, `worknotes.txt`, and `summary.txt` are removed by the entry script after final normalization.

## Do not edit generated files

Do not manually edit:

```text
incident_lifecycle.csv
worknotes_summary.txt
closing_summary.txt
state.json
```

If the output looks incorrect, stale, or incomplete, rerun the script and use the refreshed files.

## incident_lifecycle.csv columns

The final lifecycle CSV uses this simplified header order:

```text
Cluster,ProtectionGroup,Environment,Host,ObjectName,ObjectType,RunType,Status,OldestFailedET,NewestFailedET,LatestSuccessET,FailureRuns,Message
```

Column meaning:

| Column | Meaning |
|---|---|
| `Cluster` | Cohesity cluster name. |
| `ProtectionGroup` | Protection group name. |
| `Environment` | Cohesity environment category such as Oracle, SQL, Physical, HyperV, Acropolis, NAS, Isilon, RemoteAdapter. |
| `Host` | Host/source name when available. Blank when Cohesity did not return a specific host. |
| `ObjectName` | Protected object name when Cohesity returned one. Blank for run-level/protection-group-level failures where no object was returned. |
| `ObjectType` | Object type returned by Cohesity, for example `kDatabase`, `kHost`, `kVirtualMachine`, or `ProtectionGroup`. |
| `RunType` | Backup run type, for example `kRegular` or `kLog`. |
| `Status` | Lifecycle status for the row. |
| `OldestFailedET` | Oldest tracked failure timestamp for this lifecycle row. |
| `NewestFailedET` | Newest tracked failure timestamp for this lifecycle row. |
| `LatestSuccessET` | Later successful backup timestamp that cleared the row. Blank when the row is still active/unresolved. |
| `FailureRuns` | Count of unique failed backup run keys tracked for the row. |
| `Message` | Failure/detail message from Cohesity or script-generated explanation. |

## Object name handling

The script no longer copies the protection group name into `ObjectName` for run-level/protection-group-level failures.

If Cohesity did not return an object, `ObjectName` is left blank. The row can still be identified by `ProtectionGroup`, `ObjectType`, `RunType`, and `Message`.

## Latest-success reconciliation

After the main collection completes, the entry script performs final reconciliation.

A row is not allowed to remain active/unresolved when all of these are true:

```text
Status is active/unresolved
LatestRunStatus is Succeeded or SucceededWithWarning
LastSeenET is later than LastFailedET
```

When this condition is found, the entry script:

```text
1. Removes the row from active/unresolved tracking.
2. Sets Status to NewlyClearedThisCheck.
3. Sets LatestSuccessET to the successful backup time in incident_lifecycle.csv.
4. Updates state.json so the next run does not carry the row as active.
```

This prevents rows such as the following from appearing as active failures:

```text
Status = OlderStillFailing
LatestRunStatus = Succeeded
LastSeenET > LastFailedET
ClearedET = blank
```

That combination is treated as cleared by a later successful backup.

## Worknotes summary format

`worknotes_summary.txt` is summary-only and count-focused.

It contains:

- Incident/window metadata.
- Cohesity API Collection Status.
- Do-not-edit instruction.
- Summary counts.
- Active parent/child count breakdown.
- Tally checks.
- Latest-success reconciliation result.
- Incomplete collection details.
- Retry command when needed.
- Final file list.

It does not contain:

- CSV headers.
- Sort order details.
- Full row dumps.
- Current/cleared CSV references.
- Failure messages for every row.

All row-level detail is in:

```text
incident_lifecycle.csv
```

## Count model

Counts are based on the final normalized lifecycle data.

```text
Active / unresolved failures = lifecycle rows with active/unresolved Status
Cleared by later successful backup = lifecycle rows with cleared Status
Total lifecycle rows tracked = all rows in incident_lifecycle.csv
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

If this does not match, `worknotes_summary.txt` prints a `requires review` line and the team should review `incident_lifecycle.csv`.

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

Worknotes wording for `UnknownNeedsReview` is:

```text
Needs review / not verified
```

Meaning: the script could not verify whether the item cleared. Treat it as unresolved until a later successful backup is verified.

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

## Required incident attachments / updates

For each clean run:

```text
worknotes_summary.txt
incident_lifecycle.csv
```

For closure or handoff, also use:

```text
closing_summary.txt
```

Do not attach or manually edit:

```text
state.json
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
- Unknown, timeout, or incomplete API data does not clear.
- Consecutive count is based on unique failed backup runs, not script executions.
- Evaluation is limited to the latest 30 runs per protection group/run type.

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
