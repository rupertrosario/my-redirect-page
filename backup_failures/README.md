# Cohesity Backup Failure INC Status Update

## Current status

This workflow is being corrected for object-level backup failure reporting.

The current confirmed issue is:

```text
The Cohesity UI shows the failed object, but the incident workflow output is still not showing that object.
```

Do not make more collector changes blindly. First run the diagnostic probe for one affected protection group and review the actual `run.objects` shape returned by Cohesity.

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

Main collection logic must stay in:

```text
Get-CohesityBackupFailureWindowConsolidator.ps1
```

Wrapper responsibility only:

```text
Cohesity_Backup_Failure_INC_Status_Update.ps1
```

The wrapper must only call the main collector and print final file paths. Do not add object collection or reconciliation logic to the wrapper.

## Required object-level behavior

The collector must query PG runs with:

```text
/v2/data-protect/protection-groups/{pgId}/runs?numRuns=30&excludeNonRestorableRuns=false&includeObjectDetails=true
```

Rules:

```text
1. Report failures at object level, not PG level.
2. Identify the actual failed object from run.objects.
3. Object identity should use object.id when available.
4. If object.id is unavailable, use environment | objectType | object.name | sourceId.
5. If the same object has a later successful backup, do not show it as active.
6. If the same object is still failing, show actual ObjectName and ObjectType.
7. Do not copy ProtectionGroup name into ObjectName.
8. If Cohesity returns no object evidence, ObjectName and ObjectType must stay blank.
9. Worknotes must stay simple: Failure Section and Success Section only.
10. Do not add extra worknote sections.
```

## Current commits

Main collector rebuilt, but not confirmed good:

```text
5aa2fb0c2b3d673657d692585b9bafbc7ef01131
backup_failures/Get-CohesityBackupFailureWindowConsolidator.ps1
```

Wrapper simplified:

```text
785de011b536095d7ae08852db9372b04be68c8b
backup_failures/Cohesity_Backup_Failure_INC_Status_Update.ps1
```

Diagnostic probe added:

```text
facf547570d217ff3052706edb0f527154549aff
backup_failures/Test-CohesityRunObjectDetails.ps1
```

README updated with this handoff:

```text
This README section is the current handoff baseline.
```

## Normal run

```powershell
cd X:\PowerShell\Cohesity_API_Scripts\backup_failures
.\Cohesity_Backup_Failure_INC_Status_Update.ps1 -ClusterName "YOUR_CLUSTER_NAME"
```

Timeout-sensitive run:

```powershell
.\Cohesity_Backup_Failure_INC_Status_Update.ps1 -ClusterName "YOUR_CLUSTER_NAME" -RequestTimeoutSec 45
```

Optional grid view:

```powershell
.\Cohesity_Backup_Failure_INC_Status_Update.ps1 -ClusterName "YOUR_CLUSTER_NAME" -ShowGrid
```

## Diagnostic probe

Use this before the next collector code change.

```powershell
cd X:\PowerShell\Cohesity_API_Scripts
git pull --ff-only origin Cohesity_Automations

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

Paste 5-10 rows from the diagnostic CSV for one affected PG, especially the failed run rows. Then patch only `Get-CohesityBackupFailureWindowConsolidator.ps1` based on the actual API response.

## Older pasted script reference

The older pasted script was able to get object data better using:

```powershell
$runsUri = "$baseUrl/v2/data-protect/protection-groups/$pgId/runs?numRuns=10&excludeNonRestorableRuns=false&includeObjectDetails=true"
```

Useful behavior to retain:

```text
- Uses includeObjectDetails=true.
- Builds object key from environment | objectType | name | id | sourceId.
- Treats newer object entries with no failedAttempts[] as success/clear.
- For NAS/Isilon, captures any object with failedAttempts[], not only kHost.
- Has fallback logic when object-level failedAttempts[] are not returned.
```

Bad behavior to avoid:

```text
- Do not set ObjectName = ProtectionGroup name when object name is missing.
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
cluster-a | PG_SQL_PROD | SQL | sqlhost01 | DB_APP01 | kDatabase | kRegular | Failed | 2026-07-08 22:10:00 | 2026-07-08 22:10:00 |  | 1 | backup failed message
cluster-b | PG_AHV_PROD | Acropolis |  | vm-app-22 | kVirtualMachine | kRegular | Failed | 2026-07-08 23:40:00 | 2026-07-08 23:40:00 |  | 1 | backup failed message

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

## Notes

```text
- Keep it simple.
- No extra worknotes sections.
- No wrapper-side collector logic.
- Main collector owns object-level detection.
- Cohesity calls are GET-only.
- ET timestamps.
- Do not show System.Object[].
- No ServiceNow writes.
- No Excel output.
```
