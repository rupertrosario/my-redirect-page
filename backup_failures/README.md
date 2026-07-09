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

## Current final fix

Collector fix commit:

```text
df9ad3086fec028d9933d661342a663a9974fa7e
backup_failures/Get-CohesityBackupFailureWindowConsolidator.ps1
```

README cleanup commit:

```text
This file is the simplified operator handoff.
```

## What was fixed

The earlier issue was:

```text
Diagnostic output showed failed object names, but normal outputs did not show the same objects or ProtectionGroup.
```

The collector now:

```text
1. Reads failed objects from run.objects.
2. Promotes object.name to ObjectName.
3. Promotes object.objectType to ObjectType.
4. Carries ProtectionGroup from the active protection group being queried.
5. Treats object-level failedAttempts/status/error/message as failure evidence.
6. Suppresses an object from active failures only when a later successful backup for the same object exists.
7. Keeps ObjectName/ObjectType blank only when Cohesity returns no object evidence.
8. Does not copy ProtectionGroup name into ObjectName.
```

## Normal production run

```powershell
cd X:\PowerShell\Cohesity_API_Scripts

git pull --ff-only origin Cohesity_Automations

cd X:\PowerShell\Cohesity_API_Scripts\backup_failures

.\Cohesity_Backup_Failure_INC_Status_Update.ps1 `
  -ClusterName "YOUR_CLUSTER_NAME" `
  -IncidentNumber "INC1234567" `
  -RequestTimeoutSec 90
```

Default run depth:

```text
NumRuns = 30
```

Meaning:

```text
Latest 30 runs per protection group are checked.
```

Override only if needed:

```powershell
.\Cohesity_Backup_Failure_INC_Status_Update.ps1 `
  -ClusterName "YOUR_CLUSTER_NAME" `
  -IncidentNumber "INC1234567" `
  -NumRuns 50 `
  -RequestTimeoutSec 90
```

## Clean acceptance test

Use a clean output root when validating a fix so old `state.json` does not carry old rows forward:

```powershell
cd X:\PowerShell\Cohesity_API_Scripts\backup_failures

.\Cohesity_Backup_Failure_INC_Status_Update.ps1 `
  -ClusterName "YOUR_CLUSTER_NAME" `
  -IncidentNumber "INCTEST12345" `
  -OutputRoot "X:\PowerShell\Data\Cohesity\BackupFailureWindow_TEST" `
  -RequestTimeoutSec 90
```

Check these files:

```text
X:\PowerShell\Data\Cohesity\BackupFailureWindow_TEST\INCTEST12345\current_failures.csv
X:\PowerShell\Data\Cohesity\BackupFailureWindow_TEST\INCTEST12345\incident_lifecycle.csv
X:\PowerShell\Data\Cohesity\BackupFailureWindow_TEST\INCTEST12345\worknotes_summary.txt
```

Acceptance criteria:

```text
1. Failed object names appear in current_failures.csv.
2. Same object names appear in incident_lifecycle.csv.
3. Same object names appear in worknotes_summary.txt.
4. ProtectionGroup is populated.
5. ObjectName/ObjectType are blank only when Cohesity returns no object evidence.
6. ProtectionGroup is never copied into ObjectName.
```

## Output files

Operator-facing files:

```text
worknotes_summary.txt
incident_lifecycle.csv
closing_summary.txt
```

Script memory file:

```text
state.json
```

Do not manually edit `state.json` and do not attach it to the incident.

## Worknotes format

```text
Cohesity Backup Failure Incident Update

Incident: INC1234567
Compute Window: 2026-07-08 18:00 ET -> 2026-07-09 18:00 ET
Generated At: 2026-07-09 04:30:00 ET
Cohesity API Collection Status: Complete
Scope: latest 30 runs per protection group/run type.

Summary Counts:
- Active / unresolved failures: 2
- Newly cleared this check: 1
- Previously cleared rows retained in lifecycle CSV: 3
- Total lifecycle rows tracked: 6

Failure Section:
Cluster | ProtectionGroup | Environment | Host | ObjectName | ObjectType | RunType | Status | OldestFailedET | NewestFailedET | LatestSuccessET | FailureRuns | Message
cluster-a | PG_SQL_PROD | SQL | sqlhost01 | DB_APP01 | kDatabase | kRegular | OlderStillFailing | 2026-07-08 22:10:00 | 2026-07-08 22:10:00 |  | 1 | backup failed message
cluster-b | PG_AHV_PROD | Acropolis |  | vm-app-22 | kVirtualMachine | kRegular | NewlyFailedThisCheck | 2026-07-08 23:40:00 | 2026-07-08 23:40:00 |  | 1 | backup failed message

Success Section:
Cluster | ProtectionGroup | Environment | RunType | LatestSuccessET
cluster-c | PG_FS_PROD | Physical | kRegular | 2026-07-09 01:20:00
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
2. Matching rows from current_failures.csv.
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
Review worknotes_summary.txt and incident_lifecycle.csv
```
