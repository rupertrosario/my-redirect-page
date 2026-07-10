# Cohesity Backup Failures - Current Validation Instructions

## Important status

This code was pushed but has not been validated against Helios from ChatGPT. Validation must be performed on the PowerShell host.

Do not treat the report as production-ready until these checks pass.

## Current rule

Use only the wrapper for normal runs and test runs.

Do not run the collector directly unless debugging.

```text
Normal script:
backup_failures/Cohesity_Backup_Failure_INC_Status_Update.ps1

Test output root:
X:\PowerShell\Data\Cohesity\BackupFailureWindow_Test

Production output root:
X:\PowerShell\Data\Cohesity\BackupFailureWindow
```

## Files in scope

```text
backup_failures/Cohesity_Backup_Failure_INC_Status_Update.ps1
backup_failures/Get-CohesityBackupFailureWindowConsolidator.ps1
backup_failures/Format-CohesityBackupFailureReport.ps1
backup_failures/UNTESTED_VALIDATION_INSTRUCTIONS.md
```

## What the wrapper does

```text
1. Runs the main collector.
2. New INC/window uses Baseline scan with 30 runs.
3. Same INC/window rerun uses Incremental scan with 15 runs.
4. Runs the formatter after collection.
5. Produces cleaned worknotes_summary.txt and incident_lifecycle.csv.
6. Keeps raw CSV copies as *_raw.csv for troubleshooting.
```

## 1. Pull latest code

```powershell
cd X:\PowerShell\Cohesity_API_Scripts

git fetch origin
git checkout Cohesity_Automations
git pull --ff-only origin Cohesity_Automations
```

## 2. Confirm latest code is present

```powershell
cd X:\PowerShell\Cohesity_API_Scripts\backup_failures

Select-String .\Cohesity_Backup_Failure_INC_Status_Update.ps1 -Pattern `
"Incremental NumRuns","Baseline NumRuns","Format-CohesityBackupFailureReport"

Select-String .\Get-CohesityBackupFailureWindowConsolidator.ps1 -Pattern `
"BaselineNumRuns","RemoteAdapter","Clear only when the same ObjectKey"

Select-String .\Format-CohesityBackupFailureReport.ps1 -Pattern `
"StatusChange","Running / In-progress PGs","Cancelled Backup PGs","incident_lifecycle_raw.csv"
```

Expected: matches appear from all three scripts.

## 3. Syntax check

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

## 4. Safe test run

Use only the existing test folder path below.

```powershell
cd X:\PowerShell\Cohesity_API_Scripts\backup_failures

.\Cohesity_Backup_Failure_INC_Status_Update.ps1 `
  -IncidentNumber "INC999998" `
  -OutputRoot "X:\PowerShell\Data\Cohesity\BackupFailureWindow_Test" `
  -RequestTimeoutSec 120
```

Expected first test run if this test INC/state is new:

```text
RunMode = Baseline
Scan NumRuns = 30
Formatter runs after collector
```

## 5. Find the test output folder

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

## 6. Validate work notes

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

Expected note when running or cancelled count is greater than zero:

```text
Please check incident_lifecycle.csv and continue monitoring running/cancelled backups.
```

This sentence should not appear:

```text
These are not counted as failures or success.
```

## 7. Validate lifecycle columns

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
FailedRunCount
Message
```

These technical columns should not be in the cleaned lifecycle CSV:

```text
WindowKey
ObjectKey
ClusterId
ProtectionGroupId
EnvironmentFilter
FailedRunKeys
```

They should remain in:

```text
incident_lifecycle_raw.csv
state.json
```

## 8. Validate current failures

```powershell
Import-Csv (Join-Path $latest.FullName "current_failures.csv") |
  Select Status,StatusChange,Cluster,ProtectionGroup,Environment,Host,ObjectName,ObjectType,RunType,LastFailedET,FailureDates,ConsecutiveFailureDays,FailedRunCount,Message |
  Format-Table -AutoSize -Wrap
```

Pass condition:

```text
ObjectName is populated for real object failures.
ProtectionGroup is context only.
RemoteAdapter does not appear.
Same-day duplicate failures are consolidated.
```

## 9. Validate success/recovered rows

```powershell
Import-Csv (Join-Path $latest.FullName "cleared_by_success.csv") |
  Select Status,StatusChange,Cluster,ProtectionGroup,Environment,Host,ObjectName,ObjectType,LatestSuccessET,Message |
  Format-Table -AutoSize -Wrap
```

Pass condition:

```text
Only previously failed objects with newer same-object success are shown.
```

## 10. Validate warnings

```powershell
Get-Content (Join-Path $latest.FullName "collection_warnings.txt")
```

Pass condition:

```text
Warnings are clean, or API errors are explicit.
If API errors occurred, Collection Status must show Incomplete - RERUN REQUIRED.
```

## 11. Incremental rerun test

Run the exact same wrapper command again with the same test INC and same test output root.

```powershell
cd X:\PowerShell\Cohesity_API_Scripts\backup_failures

.\Cohesity_Backup_Failure_INC_Status_Update.ps1 `
  -IncidentNumber "INC999998" `
  -OutputRoot "X:\PowerShell\Data\Cohesity\BackupFailureWindow_Test" `
  -RequestTimeoutSec 120
```

Expected:

```text
RunMode = Incremental
Scan NumRuns = 15
Formatter runs after collector
```

## 12. Production run only after test passes

```powershell
cd X:\PowerShell\Cohesity_API_Scripts\backup_failures

.\Cohesity_Backup_Failure_INC_Status_Update.ps1
```

## Final pass rule

```text
Baseline test uses 30.
Incremental rerun uses 15.
worknotes_summary.txt has only summary, failures, success, and running/cancelled count note.
incident_lifecycle.csv has cleaned user-facing columns.
*_raw.csv files retain technical troubleshooting fields.
current_failures.csv is object-level.
cleared_by_success.csv only shows same-object newer success.
RemoteAdapter is excluded.
Collection warnings are explicit.
```
