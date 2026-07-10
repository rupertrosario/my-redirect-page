# DO THIS NOW - Backup Failure retry-aware fix

This is the only instruction file to follow now.

## Current status

```text
Cohesity_Backup_Failure_INC_Status_Update.ps1 = wrapper only, around 130 lines, do not edit
Get-CohesityBackupFailureWindowConsolidator.ps1 = main collector, around 900+ lines, edit this file only
Format-CohesityBackupFailureReport.ps1 = formatter, do not edit
```

## Goal

Put the retry-aware object state logic inside the consolidator, not inside the wrapper.

## Step 1 - Pull latest repo

```powershell
cd X:\PowerShell\Cohesity_API_Scripts

git fetch origin
git checkout Cohesity_Automations
git pull --ff-only origin Cohesity_Automations
```

## Step 2 - Confirm the wrapper is the clean wrapper

```powershell
cd X:\PowerShell\Cohesity_API_Scripts\backup_failures

(Get-Content .\Cohesity_Backup_Failure_INC_Status_Update.ps1).Count

Select-String .\Cohesity_Backup_Failure_INC_Status_Update.ps1 -Pattern `
"Get-RetryAwareCollectorPath","Retry-aware state rule","temporary"
```

Expected:

```text
Line count: around 130
Select-String: no matches
```

If that is true, the wrapper is correct. Do not edit it.

## Step 3 - Edit the consolidator only

Open:

```text
X:\PowerShell\Cohesity_API_Scripts\backup_failures\Get-CohesityBackupFailureWindowConsolidator.ps1
```

Search for:

```powershell
function Get-ObjectState
```

Replace the full `Get-ObjectState` function with this exact function:

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

Do not change anything else.

## Step 4 - Confirm the consolidator has the new code

```powershell
cd X:\PowerShell\Cohesity_API_Scripts\backup_failures

Select-String .\Get-CohesityBackupFailureWindowConsolidator.ps1 -Pattern `
"Retry-aware rule","Test-SuccessStatus `$RunStatus","failedAttempts is fallback"
```

Expected: all three patterns must match.

## Step 5 - Syntax check

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

## Step 6 - Test through wrapper only

```powershell
cd X:\PowerShell\Cohesity_API_Scripts\backup_failures

.\Cohesity_Backup_Failure_INC_Status_Update.ps1 `
  -IncidentNumber "INC999998" `
  -OutputRoot "X:\PowerShell\Data\Cohesity\BackupFailureWindow_Test" `
  -RequestTimeoutSec 120
```

## Step 7 - If test is good, push only the consolidator

```powershell
cd X:\PowerShell\Cohesity_API_Scripts

git diff -- backup_failures/Get-CohesityBackupFailureWindowConsolidator.ps1

git add backup_failures/Get-CohesityBackupFailureWindowConsolidator.ps1
git commit -m "Make backup object state retry-aware"
git push origin Cohesity_Automations
```

## Do not do these

```text
Do not edit the wrapper.
Do not put retry logic into the wrapper.
Do not run the collector directly for testing.
Do not use production output root until the test output root passes.
```