# Cohesity Backup Failures - Current Validation Instructions

## Important status

This code is pushed but has not been validated against Helios from ChatGPT.
Validation must be performed on the PowerShell host.

Do not treat the report as production-ready until these checks pass.

## Current rule

Use only the wrapper for normal runs and test runs.

Do not run the collector directly unless debugging.

```text
Normal script:
backup_failures/Cohesity_Backup_Failure_INC_Status_Update.ps1

Main collector/consolidator:
backup_failures/Get-CohesityBackupFailureWindowConsolidator.ps1

Test output root:
X:\PowerShell\Data\Cohesity\BackupFailureWindow_Test

Production output root:
X:\PowerShell\Data\Cohesity\BackupFailureWindow
```

## Current GitHub status

```text
Wrapper restored to normal wrapper: YES
Formatter/report cleanup pushed: YES
Runbook/instructions pushed: YES
Consolidator retry-aware code pushed directly: NO
```

The retry-aware code belongs in the consolidator:

```text
backup_failures/Get-CohesityBackupFailureWindowConsolidator.ps1
```

Specifically this function:

```text
Get-ObjectState
```

## Retry-aware rule

```text
failedAttempts inside the same object/run are not final failure by themselves.

Final object/snapshot status wins first.

If object/snapshot status says Success, the object is treated as Success even if failedAttempts exist from earlier retries in the same run.

failedAttempts are used as failure evidence only when no final object/run status is available.
```

## 1. Pull latest code

```powershell
cd X:\PowerShell\Cohesity_API_Scripts

git fetch origin
git checkout Cohesity_Automations
git pull --ff-only origin Cohesity_Automations
```

## 2. Confirm current files are present

```powershell
cd X:\PowerShell\Cohesity_API_Scripts\backup_failures

Get-ChildItem .\Cohesity_Backup_Failure_INC_Status_Update.ps1
Get-ChildItem .\Get-CohesityBackupFailureWindowConsolidator.ps1
Get-ChildItem .\Format-CohesityBackupFailureReport.ps1
Get-ChildItem .\UNTESTED_VALIDATION_INSTRUCTIONS.md
```

## 3. Confirm formatter/report cleanup is present

```powershell
cd X:\PowerShell\Cohesity_API_Scripts\backup_failures

Select-String .\Format-CohesityBackupFailureReport.ps1 -Pattern `
"StatusChange","Running / In-progress PGs","Cancelled Backup PGs","incident_lifecycle_raw.csv"
```

Expected: matches appear from the formatter.

## 4. Syntax check

```powershell
cd X:\PowerShell\Cohesity_API_Scripts

$collector = ".\backup_failures\Get-CohesityBackupFailureWindowConsolidator.ps1"
$wrapper   = ".\backup_failures\Cohesity_Backup_Failure_INC_Status_Update.ps1"
$formatter = ".\backup_failures\Format-CohesityBackupFailureReport.ps1"

[scriptblock]::Create((Get-Content $collector -Raw)) | Out-Null
[scriptblock]::Create((Get-Content $wrapper -Raw)) | Out-Null
[scriptblock]::Create((Get-Content $formatter -Raw)) | Out-Null

"Collector syntax OK"
"Wrapper syntax OK"
"Formatter syntax OK"
```

If this fails, stop.

## 5. Safe test run

Use only this test folder path.

```powershell
cd X:\PowerShell\Cohesity_API_Scripts\backup_failures

.\Cohesity_Backup_Failure_INC_Status_Update.ps1 `
  -IncidentNumber "INC999998" `
  -OutputRoot "X:\PowerShell\Data\Cohesity\BackupFailureWindow_Test" `
  -RequestTimeoutSec 120
```

Expected console output includes:

```text
RunMode = Baseline or Incremental
Formatter runs after collector
```

## 6. Find the test output folder

```powershell
$latest = Get-ChildItem "X:\PowerShell\Data\Cohesity\BackupFailureWindow_Test" -Directory |
  Where-Object { $_.Name -ne "Archive" } |
  Sort-Object LastWriteTime -Descending |
  Select-Object -First 1

"Latest folder: $($latest.FullName)"
Get-ChildItem $latest.FullName
```

Expected files include:

```text
current_failures.csv
current_failures_raw.csv
cleared_by_success.csv
cleared_by_success_raw.csv
incident_lifecycle.csv
incident_lifecycle_raw.csv
worknotes_summary.txt
collection_warnings.txt
state.json
```

## 7. Validate work notes

```powershell
Get-Content (Join-Path $latest.FullName "worknotes_summary.txt")
```

Expected:

```text
Summary:
Active Failures: <count>
Recovered Today: <count>
Running / In-progress PGs: <count>
Cancelled Backup PGs: <count>

Failure Section:
Success Section:
```

This sentence should not appear:

```text
These are not counted as failures or success.
```

## 8. Validate cleaned lifecycle columns

```powershell
Import-Csv (Join-Path $latest.FullName "incident_lifecycle.csv") |
  Select -First 1 |
  Get-Member -MemberType NoteProperty |
  Select -ExpandProperty Name
```

Expected visible columns:

```text
IncidentNumber
Status
StatusChange
Cluster
ProtectionGroup
Environment
Host
ObjectName
ObjectType
RunType
FirstFailedET
LastFailedET
LatestSuccessET
LastSeenET
FailureDates
ConsecutiveFailureDays
Message
```

These should not be in cleaned incident_lifecycle.csv:

```text
WindowKey
ObjectKey
ClusterId
ProtectionGroupId
EnvironmentFilter
FailedRunKeys
FailedRunCount
```

They should remain only in raw/state troubleshooting data.

## 9. Validate current failures

```powershell
Import-Csv (Join-Path $latest.FullName "current_failures.csv") |
  Select Status,StatusChange,Cluster,ProtectionGroup,Environment,Host,ObjectName,ObjectType,RunType,LastFailedET,FailureDates,ConsecutiveFailureDays,Message |
  Format-Table -AutoSize -Wrap
```

Pass condition:

```text
ObjectName is populated for real object failures.
ProtectionGroup is context only.
RemoteAdapter does not appear.
Same-day duplicate failures are consolidated.
Objects with final same-run success are not active failures just because failedAttempts existed earlier in that same run.
```

## 10. Validate success/recovered rows

```powershell
Import-Csv (Join-Path $latest.FullName "cleared_by_success.csv") |
  Select Status,StatusChange,Cluster,ProtectionGroup,Environment,Host,ObjectName,ObjectType,LatestSuccessET,Message |
  Format-Table -AutoSize -Wrap
```

Pass condition:

```text
Only previously failed objects with newer same-object success are shown.
```

## 11. Validate warnings

```powershell
Get-Content (Join-Path $latest.FullName "collection_warnings.txt")
```

Pass condition:

```text
Warnings are clean, or API errors are explicit.
If API errors occurred, Collection Status must show Incomplete - RERUN REQUIRED.
```

---

## Manual consolidator retry-aware update - add this at the bottom

Use this section only if the consolidator has not already been updated.

The uploaded/current consolidator still has the old `Get-ObjectState` function where `Get-FailedAttempts` is checked before final object/snapshot status. That is the retry issue.

### Replace only this function

File:

```text
X:\PowerShell\Cohesity_API_Scripts\backup_failures\Get-CohesityBackupFailureWindowConsolidator.ps1
```

Search for:

```powershell
function Get-ObjectState
```

Replace the full function with this:

```powershell
function Get-ObjectState($RunObject, [string]$RunStatus) {
    $ObjectStatuses = @(Get-ObjectStatusValues $RunObject)

    # Retry-aware rule:
    # Cohesity can record failedAttempts inside the same run/object even when
    # the final object/snapshot status is success, running, or cancelled.
    # Therefore final object/snapshot status must be evaluated before failedAttempts.
    if (@($ObjectStatuses | Where-Object { Test-SuccessStatus $_ }).Count -gt 0) {
        return 'Success'
    }

    if (@($ObjectStatuses | Where-Object { Test-FailedStatus $_ }).Count -gt 0) {
        return 'Failure'
    }

    if (@($ObjectStatuses | Where-Object { Test-CancelledStatus $_ }).Count -gt 0) {
        return 'Cancelled'
    }

    if (@($ObjectStatuses | Where-Object { Test-RunningStatus $_ }).Count -gt 0) {
        return 'Running'
    }

    # If there is no object/snapshot-level status, fall back to run-level status.
    if (Test-SuccessStatus $RunStatus) {
        return 'Success'
    }

    if (Test-CancelledStatus $RunStatus) {
        return 'Cancelled'
    }

    if (Test-RunningStatus $RunStatus) {
        return 'Running'
    }

    if (Test-FailedStatus $RunStatus) {
        return 'Failure'
    }

    # failedAttempts is fallback failure evidence only.
    # It must not override an explicit final Success/Running/Cancelled object status.
    if (@(Get-FailedAttempts $RunObject).Count -gt 0) {
        return 'Failure'
    }

    return 'Success'
}
```

### Confirm the change

```powershell
cd X:\PowerShell\Cohesity_API_Scripts\backup_failures

Select-String .\Get-CohesityBackupFailureWindowConsolidator.ps1 -Pattern `
"Retry-aware rule","Test-SuccessStatus `$RunStatus","failedAttempts is fallback"
```

Expected: all three should match.

### Syntax check after manual update

```powershell
cd X:\PowerShell\Cohesity_API_Scripts

$collector = ".\backup_failures\Get-CohesityBackupFailureWindowConsolidator.ps1"
$wrapper   = ".\backup_failures\Cohesity_Backup_Failure_INC_Status_Update.ps1"
$formatter = ".\backup_failures\Format-CohesityBackupFailureReport.ps1"

[scriptblock]::Create((Get-Content $collector -Raw)) | Out-Null
[scriptblock]::Create((Get-Content $wrapper -Raw)) | Out-Null
[scriptblock]::Create((Get-Content $formatter -Raw)) | Out-Null

"Collector syntax OK"
"Wrapper syntax OK"
"Formatter syntax OK"
```

### Test after manual update

```powershell
cd X:\PowerShell\Cohesity_API_Scripts\backup_failures

.\Cohesity_Backup_Failure_INC_Status_Update.ps1 `
  -IncidentNumber "INC999998" `
  -OutputRoot "X:\PowerShell\Data\Cohesity\BackupFailureWindow_Test" `
  -RequestTimeoutSec 120
```
