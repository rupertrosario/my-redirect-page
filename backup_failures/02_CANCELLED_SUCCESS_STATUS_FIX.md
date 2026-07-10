# Cancelled/success status handling correction

This note documents the correction made after identifying that a cancelled/run-level state must not override an explicit successful object/snapshot result.

## Rule

```text
Final object/snapshot success wins before failedAttempts, running, or cancelled state.
SucceededWithWarning also counts as success.
```

## Required consolidator change

In `backup_failures/Get-CohesityBackupFailureWindowConsolidator.ps1`, `Test-SuccessStatus` must include warning-success and success aliases:

```powershell
function Test-SuccessStatus([string]$Status) { (Clean $Status) -in @('Succeeded','kSucceeded','SucceededWithWarning','kSucceededWithWarning','Success','kSuccess','Successful','Completed','kCompleted') }
```

The retry-aware `Get-ObjectState` must continue to evaluate object/snapshot success before failed/cancelled/running/failure-attempt evidence:

```powershell
function Get-ObjectState($RunObject, [string]$RunStatus) {
    $ObjectStatuses = @(Get-ObjectStatusValues $RunObject)

    # Retry-aware rule:
    # Cohesity can record failedAttempts inside the same run/object even when
    # the final object/snapshot status is success, running, or cancelled.
    # Therefore final object/snapshot status must be evaluated before failedAttempts.
    # A cancelled/run-level state must not override an explicit object/snapshot success.
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

    if (@(Get-FailedAttempts $RunObject).Count -gt 0) {
        return 'Failure'
    }

    return 'Success'
}
```

## Local verification

```powershell
cd X:\PowerShell\Cohesity_API_Scripts\backup_failures

Select-String .\Get-CohesityBackupFailureWindowConsolidator.ps1 -Pattern `
"SucceededWithWarning","cancelled/run-level state must not override","Retry-aware rule"
```

Expected: all three patterns should match.
