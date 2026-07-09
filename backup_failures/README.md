# Cohesity Backup Failure INC Status Update

## Current status

This workflow is being corrected for object-level backup failure reporting.

Latest fix pushed:

```text
df9ad3086fec028d9933d661342a663a9974fa7e
backup_failures/Get-CohesityBackupFailureWindowConsolidator.ps1
```

What the fix does:

```text
1. Promotes failed objects from run.objects into normal collector output.
2. Uses object.name and object.objectType the same way the diagnostic probe does.
3. Treats object-level failedAttempts/status/error evidence as a failed object even when the parent run is not simply Failed.
4. Carries ProtectionGroup from the active protection group being queried.
5. Keeps ObjectName/ObjectType blank only when Cohesity returns no object evidence.
6. Does not copy ProtectionGroup name into ObjectName.
7. Keeps wrapper unchanged.
```

The previous confirmed issue was:

```text
Diagnostic output had failed object names.
current_failures.csv / incident_lifecycle.csv / worknotes_summary.txt did not show the same objects.
ProtectionGroup was also missing or not consistently carried for those rows.
```

The defect was in the main collector mapping/export path, not in the Cohesity API response.

## Repo context

```text
Repo:   rupertrosario/my-redirect-page
Branch: Cohesity_Automations
Folder: backup_failures/
```

## Main files

```text
Get-CohesityBackupFailureWindowConsolidator.ps1
Cohesity_Backup_Failure_INC_Status_Update.ps1
Test-CohesityRunObjectDetails.ps1
README.md
```

## Script responsibility

Main collection logic stays in:

```text
Get-CohesityBackupFailureWindowConsolidator.ps1
```

Wrapper responsibility only:

```text
Cohesity_Backup_Failure_INC_Status_Update.ps1
```

The wrapper only calls the main collector and prints final file paths. Do not add object collection or reconciliation logic to the wrapper.

## Required object-level behavior

The collector queries PG runs with:

```text
/v2/data-protect/protection-groups/{pgId}/runs?numRuns=30&excludeNonRestorableRuns=false&includeObjectDetails=true
```

Rules:

```text
1. Report failures at object level, not PG level.
2. Identify the actual failed object from run.objects.
3. Object identity uses object.id when available.
4. If object.id is unavailable, use environment | objectType | object.name | sourceId.
5. If the same object has a later successful backup, do not show it as active.
6. If the same object is still failing, show actual ObjectName and ObjectType.
7. Do not copy ProtectionGroup name into ObjectName.
8. If Cohesity returns no object evidence, ObjectName and ObjectType must stay blank.
9. Worknotes must stay simple: Failure Section and Success Section only.
10. Do not add extra worknote sections.
11. If the diagnostic probe has ObjectName/ObjectType for a failed run, the main collector must carry the same ObjectName/ObjectType into current_failures.csv, incident_lifecycle.csv, and worknotes_summary.txt.
12. ProtectionGroup must be carried from the active PG being queried.
```

## Fix validation command

Run against a clean output root first, so old state does not carry blank historical rows forward:

```powershell
cd X:\PowerShell\Cohesity_API_Scripts
git pull --ff-only origin Cohesity_Automations

cd X:\PowerShell\Cohesity_API_Scripts\backup_failures
.\Cohesity_Backup_Failure_INC_Status_Update.ps1 `
  -ClusterName "YOUR_CLUSTER_NAME" `
  -IncidentNumber "INCTEST12345" `
  -OutputRoot "X:\PowerShell\Data\Cohesity\BackupFailureWindow_TEST" `
  -RequestTimeoutSec 90
```

Then check:

```text
X:\PowerShell\Data\Cohesity\BackupFailureWindow_TEST\INCTEST12345\current_failures.csv
X:\PowerShell\Data\Cohesity\BackupFailureWindow_TEST\INCTEST12345\incident_lifecycle.csv
X:\PowerShell\Data\Cohesity\BackupFailureWindow_TEST\INCTEST12345\worknotes_summary.txt
```

Expected result:

```text
Diagnostic CSV ObjectName == current_failures.csv ObjectName
Diagnostic CSV ObjectType == current_failures.csv ObjectType
ProtectionGroup is populated in current_failures.csv and worknotes_summary.txt
ObjectName/ObjectType are blank only if run.objects has no object evidence
```

## Diagnostic probe

Use this if the output still does not match the Cohesity UI:

```powershell
cd X:\PowerShell\Cohesity_API_Scripts\backup_failures
.\Test-CohesityRunObjectDetails.ps1 -ClusterName "YOUR_CLUSTER_NAME" -ProtectionGroupName "YOUR_PG_NAME" -NumRuns 10
```

If the PG name is duplicated:

```powershell
.\Test-CohesityRunObjectDetails.ps1 -ClusterName "YOUR_CLUSTER_NAME" -ProtectionGroupId "YOUR_PG_ID" -NumRuns 10
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

## Final operator-facing files

```text
worknotes_summary.txt
incident_lifecycle.csv
closing_summary.txt
```

`state.json` is script memory. Do not manually edit it and do not attach it to the incident.

## Target worknotes_summary.txt format

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

## incident_lifecycle.csv columns

```text
Cluster,ProtectionGroup,Environment,Host,ObjectName,ObjectType,RunType,Status,OldestFailedET,NewestFailedET,LatestSuccessET,FailureRuns,Message
```

For missing object evidence:

```text
ObjectName must be blank.
ObjectType must be blank.
Do not copy PG name into ObjectName.
```
