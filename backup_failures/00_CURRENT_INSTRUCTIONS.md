# CURRENT INSTRUCTIONS - Backup Failure workflow

This is the only active instruction file for `backup_failures/`.

Going forward, overwrite this file whenever instructions change. Do not create numbered instruction files such as `02_...`, `03_...`, or `04_...` for normal guidance. Old instruction files are obsolete and must not be used as the source of truth.

## Repository scope

- Repo: `rupertrosario/my-redirect-page`
- Branch: `Cohesity_Automations`
- Folder: `backup_failures/`

## Current state

We are going back to basics.

Do not continue broad all-cluster testing until the core lifecycle logic is proven on one known object.

The current active problem is the consolidator logic and validation, not the wrapper and not the formatter.

## Files and ownership

### Wrapper

File: `backup_failures/Cohesity_Backup_Failure_INC_Status_Update.ps1`

Rules:
- Keep wrapper small.
- Wrapper should call the consolidator.
- Wrapper should run formatter only after consolidator succeeds.
- Do not add collection logic, patching logic, or lifecycle logic to wrapper.

### Formatter

File: `backup_failures/Format-CohesityBackupFailureReport.ps1`

Rules:
- Formatter only formats consolidator output.
- Formatter should not call Cohesity APIs.
- Formatter should not decide object lifecycle.
- Formatter may normalize legacy/new statuses for reporting.

### Consolidator

File: `backup_failures/Get-CohesityBackupFailureWindowConsolidator.ps1`

Rules:
- Consolidator owns Cohesity API collection.
- Consolidator owns object identity.
- Consolidator owns failure/running/cancelled/success lifecycle logic.
- Consolidator owns state comparison.
- Consolidator must report object-level backup state, not only protection-group-level failure.

## First verification before any test

On the machine running the workflow, verify the actual local consolidator file before testing:

```powershell
cd X:\PowerShell\Cohesity_API_Scripts\backup_failures

Get-FileHash .\Get-CohesityBackupFailureWindowConsolidator.ps1 -Algorithm SHA256

Select-String .\Get-CohesityBackupFailureWindowConsolidator.ps1 -Pattern `
"Recovery identity rule","ObjectKey must not include RunType","Recovery-aware rule","Processing clusters alphabetically"
```

Expected for the latest intended local V4 file:

```text
Recovery identity rule             = match
ObjectKey must not include RunType = match
Recovery-aware rule                = match
Processing clusters alphabetically = no match
```

If `Processing clusters alphabetically` appears, the machine is running an old consolidator.

## Back-to-basics lifecycle goal

The script must answer this question for each protected object:

```text
Is this object still in a bad backup state after checking the latest backup runs?
```

It must not answer only:

```text
Did this object ever have a failed/cancelled/running run?
```

## Correct lifecycle rules

| Evidence | Expected result |
|---|---|
| Failed backup, no later success | Active/current problem |
| Cancelled backup, no later success | Active/current problem |
| Running backup, no later final result | Active/current problem |
| Failed backup, later success | Cleared / recovered |
| Cancelled backup, later success | Cleared / recovered |
| Running backup, later success | Cleared / recovered |
| Success only | Not reported as problem |
| Previously open in state, now success | Cleared / recovered |

## Correct object identity rule

Object lifecycle matching should be based on:

```text
Cluster + ProtectionGroup + Environment + ObjectIdentity
```

Object lifecycle matching should not depend on `RunType`.

Reason:

```text
Same object cancelled in Incremental
Same object later successful in Full/Synthetic/Incremental
```

The later success should clear the object because the object recovered.

`RunType` should still remain in report rows for audit context, but it should not prevent recovery matching.

## Testing rule

Do not test lifecycle logic with all clusters and 15/30 runs.

Use one known cluster and one known protection group/object where the problem is visible:

```text
Cancelled first
Successful later
Same protected object
```

Run the consolidator directly for a small test:

```powershell
cd X:\PowerShell\Cohesity_API_Scripts\backup_failures

.\Get-CohesityBackupFailureWindowConsolidator.ps1 `
  -IncidentNumber "INC999998" `
  -OutputRoot "X:\PowerShell\Data\Cohesity\BackupFailureWindow_Test" `
  -ClusterName "<EXACT_CLUSTER_NAME>" `
  -NumRuns 3 `
  -BaselineNumRuns 3 `
  -RequestTimeoutSec 60
```

Then run formatter:

```powershell
$latest = Get-ChildItem "X:\PowerShell\Data\Cohesity\BackupFailureWindow_Test" -Directory |
  Where-Object { $_.Name -ne "Archive" } |
  Sort-Object LastWriteTime -Descending |
  Select-Object -First 1

.\Format-CohesityBackupFailureReport.ps1 -ReportFolder $latest.FullName
```

## Inspect one known object

Replace `<OBJECT_NAME>` with the object that had cancelled first and later success:

```powershell
$latest = Get-ChildItem "X:\PowerShell\Data\Cohesity\BackupFailureWindow_Test" -Directory |
  Where-Object { $_.Name -ne "Archive" } |
  Sort-Object LastWriteTime -Descending |
  Select-Object -First 1

"Latest folder: $($latest.FullName)"

$objectName = "<OBJECT_NAME>"

"--- lifecycle ---"
Import-Csv (Join-Path $latest.FullName "incident_lifecycle.csv") |
  Where-Object { $_.ObjectName -like "*$objectName*" } |
  Select Cluster,ProtectionGroup,Environment,ObjectName,ObjectType,RunType,Status,StatusChange,LastSeenET,LatestSuccessET,Message |
  Format-Table -AutoSize

"--- cleared ---"
Import-Csv (Join-Path $latest.FullName "cleared_by_success.csv") |
  Where-Object { $_.ObjectName -like "*$objectName*" } |
  Select Cluster,ProtectionGroup,Environment,ObjectName,ObjectType,RunType,Status,StatusChange,LatestSuccessET,Message |
  Format-Table -AutoSize

"--- current failures ---"
Import-Csv (Join-Path $latest.FullName "current_failures.csv") |
  Where-Object { $_.ObjectName -like "*$objectName*" } |
  Select Cluster,ProtectionGroup,Environment,ObjectName,ObjectType,RunType,Status,StatusChange,LastSeenET,Message |
  Format-Table -AutoSize
```

## Expected result for cancelled-then-success

The object should appear in:

```text
cleared_by_success.csv
```

with:

```text
Status = Success
StatusChange = Cleared
```

It should not remain active in:

```text
current_failures.csv
```

as:

```text
Cancelled
CancelledAfterFailure
Failure
```

## Performance rule

For testing:

```text
NumRuns = 3
BaselineNumRuns = 3
One cluster only
```

For production defaults, do not assume 15/30 is acceptable. Decide after lifecycle logic is proven.

Candidate production defaults after validation:

```text
Incremental NumRuns = 5
BaselineNumRuns = 10 or 15
```

## GitHub/source-of-truth rule

Do not claim the full consolidator is pushed unless it has been explicitly fetched from GitHub after update and verified.

For now, this instruction file is the source of truth for what to test and how to reason about the lifecycle.

## Operating rule going forward

When instructions change:

1. Overwrite `backup_failures/00_CURRENT_INSTRUCTIONS.md`.
2. Do not create a new numbered instruction file.
3. If old instruction files exist, treat them as obsolete.
4. Keep chat instructions and this file aligned.
