# AFTER CURRENT RUN COMPLETES - Backup Failure validation and push steps

Follow this file after the current wrapper run finishes.

Do **not** edit or pull files while the script is still running.

## Current rule

```text
Cohesity_Backup_Failure_INC_Status_Update.ps1 = wrapper only, around 130 lines, do not edit
Get-CohesityBackupFailureWindowConsolidator.ps1 = main collector, around 900+ lines, retry-aware fix belongs here
Format-CohesityBackupFailureReport.ps1 = formatter, do not edit
```

## Step 1 - Confirm the run finished

The PowerShell prompt should return. Do not continue if the collector is still printing environments, clusters, or warnings.

## Step 2 - Find the latest test output folder

```powershell
$latest = Get-ChildItem "X:\PowerShell\Data\Cohesity\BackupFailureWindow_Test" -Directory |
  Where-Object { $_.Name -ne "Archive" } |
  Sort-Object LastWriteTime -Descending |
  Select-Object -First 1

"Latest folder: $($latest.FullName)"

Get-ChildItem $latest.FullName |
  Select Name,Length,LastWriteTime |
  Format-Table -AutoSize
```

Expected files:

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
closing_summary.txt
```

## Step 3 - Check warnings

```powershell
Get-Content (Join-Path $latest.FullName "collection_warnings.txt")
```

Pass condition:

```text
None
```

If API errors are listed, do not push yet. Capture the warning text.

## Step 4 - Check worknotes

```powershell
Get-Content (Join-Path $latest.FullName "worknotes_summary.txt")
```

Expected sections:

```text
Summary:
Active Failures:
Recovered Today:
Running / In-progress PGs:
Cancelled Backup PGs:
Failure Section:
Success Section:
```

## Step 5 - Confirm wrapper is clean

```powershell
cd X:\PowerShell\Cohesity_API_Scripts\backup_failures

(Get-Content .\Cohesity_Backup_Failure_INC_Status_Update.ps1).Count

Select-String .\Cohesity_Backup_Failure_INC_Status_Update.ps1 -Pattern `
"Get-RetryAwareCollectorPath","Retry-aware state rule","temporary"
```

Expected:

```text
Line count around 130
No Select-String matches
```

## Step 6 - Confirm consolidator has retry-aware logic

```powershell
Select-String .\Get-CohesityBackupFailureWindowConsolidator.ps1 -Pattern `
"Retry-aware rule","Test-SuccessStatus `$RunStatus","failedAttempts is fallback"
```

Expected: all three patterns match.

## Step 7 - Syntax check

```powershell
cd X:\PowerShell\Cohesity_API_Scripts

$collector = ".\backup_failures\Get-CohesityBackupFailureWindowConsolidator.ps1"
$wrapper   = ".\backup_failures\Cohesity_Backup_Failure_INC_Status_Update.ps1"
$formatter = ".\backup_failures\Format-CohesityBackupFailureReport.ps1"

[scriptblock]::Create((Get-Content $collector -Raw)) | Out-Null
[scriptblock]::Create((Get-Content $wrapper -Raw)) | Out-Null
[scriptblock]::Create((Get-Content $formatter -Raw)) | Out-Null

"All syntax OK"
```

## Step 8 - Push only the consolidator if all checks pass

```powershell
cd X:\PowerShell\Cohesity_API_Scripts

git status --short

git diff -- backup_failures/Get-CohesityBackupFailureWindowConsolidator.ps1

git add backup_failures/Get-CohesityBackupFailureWindowConsolidator.ps1
git commit -m "Make backup object state retry-aware"
git push origin Cohesity_Automations
```

Do not add test output files. Do not add backup `.bak_*` files.

## If the run fails

Do not push. Capture:

```powershell
Get-Content (Join-Path $latest.FullName "collection_warnings.txt")
Get-Content (Join-Path $latest.FullName "worknotes_summary.txt")
git status --short
```

Also copy the last 30 console lines from the failed run.