# Retry-aware consolidator patch - shape changed fallback

Use this file when the earlier patch says:

```text
collector function shape changed
```

That message means the exact old `Get-ObjectState` text did not match. It does **not** mean the consolidator is broken.

This fallback replaces the block from:

```powershell
function Get-ObjectState
```

up to just before:

```powershell
function Test-TargetObject
```

## Run this fallback patch

```powershell
cd X:\PowerShell\Cohesity_API_Scripts\backup_failures

$path = ".\Get-CohesityBackupFailureWindowConsolidator.ps1"

$content = Get-Content $path -Raw

$backup = "$path.bak_$(Get-Date -Format 'yyyyMMdd_HHmmss')"
Copy-Item $path $backup -Force

$newFunction = @'
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
'@

$pattern = '(?s)function Get-ObjectState\s*\(\$RunObject,\s*\[string\]\$RunStatus\)\s*\{.*?\}\s*(?=function Test-TargetObject)'

if ($content -notmatch $pattern) {
    throw "Could not locate Get-ObjectState block before Test-TargetObject. Stop and send the Get-ObjectState section."
}

$content = [regex]::Replace($content, $pattern, ($newFunction + "`r`n`r`n"), 1)

Set-Content -Path $path -Value $content -Encoding UTF8

[scriptblock]::Create((Get-Content $path -Raw)) | Out-Null

"Updated Get-ObjectState successfully"
"Backup created: $backup"
```

## Confirm the change

```powershell
Select-String .\Get-CohesityBackupFailureWindowConsolidator.ps1 -Pattern `
"Retry-aware rule","Test-SuccessStatus `$RunStatus","failedAttempts is fallback"
```

Expected: all three should show.

## Syntax check

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

## Test through wrapper only

```powershell
cd X:\PowerShell\Cohesity_API_Scripts\backup_failures

.\Cohesity_Backup_Failure_INC_Status_Update.ps1 `
  -IncidentNumber "INC999998" `
  -OutputRoot "X:\PowerShell\Data\Cohesity\BackupFailureWindow_Test" `
  -RequestTimeoutSec 120
```
