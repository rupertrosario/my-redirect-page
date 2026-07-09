# Cohesity Backup Failure INC Status Update

## Production baseline

Use this as the production entry point:

```powershell
Cohesity_Backup_Failure_INC_Status_Update.ps1
```

The wrapper calls the main collector:

```powershell
Get-CohesityBackupFailureWindowConsolidator.ps1
```

Diagnostic script is only for troubleshooting:

```powershell
Test-CohesityRunObjectDetails.ps1
```

## Current commits

Current collector fix:

```text
adaeffed56b9be9a120e83f20ff66ac400955d16
backup_failures/Get-CohesityBackupFailureWindowConsolidator.ps1
```

Wrapper production defaults and PG-level cleanup:

```text
c1ff8493e4f0365e609e06872ef9c93fb65b03ec
backup_failures/Cohesity_Backup_Failure_INC_Status_Update.ps1
```

## Normal production run

Run from the normal folder. Do not pass `OutputRoot` unless testing.

```powershell
cd X:\PowerShell\Cohesity_API_Scripts

git pull --ff-only origin Cohesity_Automations

cd X:\PowerShell\Cohesity_API_Scripts\backup_failures

.\Cohesity_Backup_Failure_INC_Status_Update.ps1
```

Default behavior:

```text
No -ClusterName       = scan all clusters
No -IncidentNumber    = ask once, then reuse for the current 18:00 ET backup-failure window
No -OutputRoot        = use X:\PowerShell\Data\Cohesity\BackupFailureWindow
No -RequestTimeoutSec = use 120 seconds from wrapper code
No -NumRuns           = use 20 runs per protection group from wrapper code
```

## Optional commands

Run one cluster only:

```powershell
.\Cohesity_Backup_Failure_INC_Status_Update.ps1 `
  -ClusterName "YOUR_CLUSTER_NAME"
```

Run with explicit incident number:

```powershell
.\Cohesity_Backup_Failure_INC_Status_Update.ps1 `
  -IncidentNumber "INC1234567"
```

Run with more PG runs only if needed:

```powershell
.\Cohesity_Backup_Failure_INC_Status_Update.ps1 `
  -NumRuns 30
```

Run faster with lower run depth only if accepted operationally:

```powershell
.\Cohesity_Backup_Failure_INC_Status_Update.ps1 `
  -NumRuns 10
```

## Incident handling

The script uses a backup-failure window starting at 18:00 ET.

For the first run in a window:

```text
If -IncidentNumber is not supplied, the script prompts for the incident number.
```

For later runs in the same window:

```text
The script reuses the incident number from BackupFailure_WindowRegistry.json.
```

Registry path:

```text
X:\PowerShell\Data\Cohesity\BackupFailureWindow\BackupFailure_WindowRegistry.json
```

Do not manually edit the registry unless intentionally correcting a bad incident mapping.

## Output folder

Normal production output root:

```text
X:\PowerShell\Data\Cohesity\BackupFailureWindow
```

Incident-specific output folder:

```text
X:\PowerShell\Data\Cohesity\BackupFailureWindow\INC1234567
```

## Output files

Operator-facing files:

```text
worknotes_summary.txt
incident_lifecycle.csv
closing_summary.txt
```

Additional validation/output file:

```text
cleared_by_success.csv
```

Script memory file:

```text
state.json
```

Do not manually edit `state.json` and do not attach it to the incident.

## Object-level rule

Object-level rows are the source of truth.

```text
If run.objects has failed object evidence:
  report object-level rows only.

If run.objects has objects but no explicit failed object evidence:
  report object rows as review rows.

If run.objects has no objects at all:
  only then allow blank PG/run-level review row.
```

The wrapper also removes stale blank PG/run-level rows created by older runs:

```text
1. Blank ObjectName/ObjectType rows are removed if object-level rows exist for the same Cluster + ProtectionGroup + Environment + RunType.
2. Blank PG/run-level rows with stale statuses are removed:
   - NewlyFailedThisCheck
   - OlderStillFailing
   - CurrentStillFailing
   - CarriedForwardStillFailing
   - ReFailedAfterClear
3. True no-object-evidence review rows are still allowed.
```

This prevents old PG-level rows from remaining as `CarriedForwardStillFailing` when object-level evidence exists.

## What was fixed

The original issue was:

```text
Diagnostic output showed failed object names, but normal outputs did not show the same objects or ProtectionGroup.
```

The current behavior is:

```text
1. Reads failed objects from run.objects.
2. Promotes object.name to ObjectName.
3. Promotes object.objectType to ObjectType.
4. Carries ProtectionGroup from the active protection group being queried.
5. Treats object-level failedAttempts/status/error/message as failure evidence.
6. Suppresses newer-success-cleared objects from active Failure Section.
7. Retains same-scan cleared objects in cleared_by_success.csv, incident_lifecycle.csv, and Success Section.
8. Removes stale PG-level carried-forward rows when object-level rows exist.
9. Keeps ObjectName/ObjectType blank only when Cohesity returns no object evidence.
10. Does not copy ProtectionGroup name into ObjectName.
```

## Acceptance criteria

After running, check:

```text
current_failures.csv
cleared_by_success.csv
incident_lifecycle.csv
worknotes_summary.txt
```

Expected:

```text
1. Active failed objects appear in current_failures.csv.
2. Active failed objects appear in Failure Section of worknotes_summary.txt.
3. Failed objects that already have a newer success appear in cleared_by_success.csv.
4. Same-scan cleared objects appear in Success Section of worknotes_summary.txt.
5. All failed/cleared object evidence appears in incident_lifecycle.csv.
6. ProtectionGroup is populated.
7. ObjectName/ObjectType are blank only when Cohesity returns no object evidence.
8. ProtectionGroup is never copied into ObjectName.
9. PG-level blank rows do not appear when object-level rows exist for the same PG/run type.
10. Old PG-level `CarriedForwardStillFailing` rows are not retained.
```

## Worknotes format

```text
Cohesity Backup Failure Incident Update

Incident: INC1234567
Compute Window: 2026-07-08 18:00 ET -> 2026-07-09 18:00 ET
Generated At: 2026-07-09 04:30:00 ET
Cohesity API Collection Status: Complete
Scope: latest 20 runs per protection group/run type.

Summary Counts:
- Active / unresolved failures: 1
- Newly cleared this check: 1
- Previously cleared rows retained in lifecycle CSV: 0
- Total lifecycle rows tracked: 2

Failure Section:
Cluster | ProtectionGroup | Environment | Host | ObjectName | ObjectType | RunType | Status | OldestFailedET | NewestFailedET | LatestSuccessET | FailureRuns | Message
cluster-a | PG_SQL_PROD | SQL | sqlhost01 | DB_APP01 | kDatabase | kRegular | OlderStillFailing | 2026-07-08 22:10:00 | 2026-07-08 22:10:00 |  | 1 | backup failed message

Success Section:
Cluster | ProtectionGroup | Environment | Host | ObjectName | ObjectType | RunType | LatestSuccessET | Message
cluster-a | PG_SQL_PROD | SQL | sqlhost02 | DB_APP02 | kDatabase | kRegular | 2026-07-09 01:20:00 | backup failed message from earlier failed run
```

## Troubleshooting only

Use diagnostic only if production output still does not match Cohesity UI:

```powershell
cd X:\PowerShell\Cohesity_API_Scripts\backup_failures

.\Test-CohesityRunObjectDetails.ps1 `
  -ClusterName "YOUR_CLUSTER_NAME" `
  -ProtectionGroupName "YOUR_PG_NAME" `
  -NumRuns 10
```

If PG name is duplicated:

```powershell
.\Test-CohesityRunObjectDetails.ps1 `
  -ClusterName "YOUR_CLUSTER_NAME" `
  -ProtectionGroupId "YOUR_PG_ID" `
  -NumRuns 10
```

Diagnostic output path:

```text
X:\PowerShell\Data\Cohesity\BackupFailureWindow\Debug\Cohesity_RunObjectDetails_<cluster>_<pg>_<timestamp>.csv
X:\PowerShell\Data\Cohesity\BackupFailureWindow\Debug\Cohesity_RunObjectDetails_<cluster>_<pg>_<timestamp>.json
```

Important diagnostic columns:

```text
RunStatus
ObjectName
ObjectType
ObjectEnvironment
ObjectId
SourceId
ParentId
LocalSnapshotInfoPresent
LocalSnapshotStatus
SnapshotStatus
SnapshotError
FailedAttemptsCount
FailedMessage
```

If production output fails after the current fix, collect only:

```text
1. 3-5 failed rows from diagnostic CSV.
2. Matching rows from current_failures.csv and cleared_by_success.csv.
3. Console error/output if any.
```

Then patch only:

```text
Get-CohesityBackupFailureWindowConsolidator.ps1
```

## Files in this folder

```text
Cohesity_Backup_Failure_INC_Status_Update.ps1      Production entry point
Get-CohesityBackupFailureWindowConsolidator.ps1   Main collector logic
Test-CohesityRunObjectDetails.ps1                 Diagnostic only
README.md                                         Operator handoff
```

## Final rule

Do not keep changing the wrapper or adding new folders.

Production use is:

```text
Run Cohesity_Backup_Failure_INC_Status_Update.ps1
Review worknotes_summary.txt, current_failures.csv, cleared_by_success.csv, and incident_lifecycle.csv
```
