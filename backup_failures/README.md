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

The final lifecycle CSV uses this header order:

```text
Cluster,ProtectionGroup,Environment,Host,ObjectName,ObjectType,RunType,Status,OldestFailedET,NewestFailedET,LatestSuccessET,FailureRuns,Message
```

Object handling:

```text
If Cohesity did not return an object, ObjectName is blank and ObjectType is blank.
The script does not copy ProtectionGroup into ObjectName.
```

## Worknotes output

`worknotes_summary.txt` contains the lifecycle output sectionwise.

It has:

```text
Summary Counts
Tally Check
Team Focus
Failure Section
Success Section
Incomplete Collection
Files to Attach / Update
Script Memory
```

### Failure Section

Failure Section contains active/unresolved lifecycle rows.

Columns:

```text
Cluster | ProtectionGroup | Environment | Host | ObjectName | ObjectType | RunType | Status | OldestFailedET | NewestFailedET | LatestSuccessET | FailureRuns | Message
```

Sort:

```text
NewestFailedET descending
```

Team focus line:

```text
Focus on OlderStillFailing and UnknownNeedsReview rows in the Failure section.
```

### Success Section

Success Section shows only rows newly cleared in the current check.

Included status:

```text
NewlyClearedThisCheck
```

Excluded from worknotes Success Section:

```text
ClearedByLaterSuccess
```

`ClearedByLaterSuccess` rows remain in `incident_lifecycle.csv` and `state.json`, but they are not pasted into worknotes because they were already known as cleared.

Success Section columns:

```text
Cluster | ProtectionGroup | Environment | Host | ObjectName | ObjectType | RunType | LatestSuccessET
```

The Success Section does not show status, old/new failure time, message, or failure count.

## Count model

Counts are based on the final normalized lifecycle data.

```text
Active / unresolved failures = lifecycle rows with active/unresolved Status
Newly cleared this check = lifecycle rows with Status NewlyClearedThisCheck
Previously cleared rows retained in lifecycle CSV = lifecycle rows with Status ClearedByLaterSuccess
Total lifecycle rows tracked = all rows in incident_lifecycle.csv
```

The lifecycle tally should normally be:

```text
Active / unresolved failures + Newly cleared this check + Previously cleared retained = Total lifecycle rows tracked
```

## Status values

```text
NewlyFailedThisCheck
OlderStillFailing
CarriedForwardStillFailing
NewlyClearedThisCheck
ClearedByLaterSuccess
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

## Closing summary

`closing_summary.txt` uses the same section model as worknotes and retains:

```text
Carry Forward / Handoff
```

Example:

```text
Carry Forward / Handoff:
5 active/unresolved rows remain in incident_lifecycle.csv and should be carried forward or separately tracked.
```

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
