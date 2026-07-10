# Cohesity Backup Failures - Current Validation Instructions

## Status

This code was pushed to GitHub, but it has not been validated against Helios from ChatGPT. Validation must be done on the PowerShell host.

Do not treat the report as production-ready until the checks below pass.

## Current latest position

Use the wrapper for normal operation and for the main test path.

```text
Normal script to run:
backup_failures/Cohesity_Backup_Failure_INC_Status_Update.ps1
```

The wrapper now does this:

```text
1. Calls Get-CohesityBackupFailureWindowConsolidator.ps1
2. Collector performs Cohesity GET-only collection
3. New window / new INC uses Baseline scan with 30 runs
4. Same window / same INC uses Incremental scan with 15 runs
5. Calls Format-CohesityBackupFailureReport.ps1
6. Formatter rewrites operator-facing CSVs and worknotes_summary.txt
```

The formatter is post-processing only:

```text
- Does not call Cohesity
- Does not change state.json
- Keeps raw CSV copies as *_raw.csv
- Renames Change to StatusChange in operator-facing CSVs
- Removes technical columns from operator-facing incident_lifecycle.csv
- Adds Running / In-progress PGs count in worknotes_summary.txt
- Adds Cancelled Backup PGs count in worknotes_summary.txt
```

## Repository context

```text
Repo:   rupertrosario/my-redirect-page
Branch: Cohesity_Automations
Folder: backup_failures/
```

## Current files

```text
backup_failures/Cohesity_Backup_Failure_INC_Status_Update.ps1
backup_failures/Get-CohesityBackupFailureWindowConsolidator.ps1
backup_failures/Format-CohesityBackupFailureReport.ps1
backup_failures/UNTESTED_VALIDATION_INSTRUCTIONS.md
```

## Latest relevant commits

```text
1f70fba67e63ca8143d1f54efb1b209ca4067557
Implement daily baseline and incremental object failure tracking

15ebce303819998d8e6f2a0946eb5d6c66f0c539
Align backup failure wrapper with baseline incremental collector

7336fd59dddc53d0225523d9b85ebd8ac3c7369a
Add backup failure report formatter

8ac82207fbb8b401caa6fa9b01e981be4aefe3b7
Run backup failure report formatter from wrapper

15e28ccb358ef073fa34aba511dc41a6972b017a
Update validation instructions for formatter flow
```

## 1. Pull latest code

```powershell
cd X:\PowerShell\Cohesity_API_Scripts

git fetch origin
git checkout Cohesity_Automations
git pull --ff-only origin Cohesity_Automations
```

## 2. Confirm latest files are present

```powershell
cd X:\PowerShell\Cohesity_API_Scripts\backup_failures

Select-String .\Get-CohesityBackupFailureWindowConsolidator.ps1 -Pattern `
"BaselineNumRuns","RemoteAdapter","Final reporting is object-level only","Clear only when the same ObjectKey"

Select-String .\Cohesity_Backup_Failure_INC_Status_Update.ps1 -Pattern `
"Incremental NumRuns","Baseline NumRuns","Format-CohesityBackupFailureReport"

Select-String .\Format-CohesityBackupFailureReport.ps1 -Pattern `
"StatusChange","Running / In-progress PGs","Cancelled Backup PGs","incident_lifecycle_raw.csv"
```

Expected: matches from all three files.

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

## 4. Safe test command

Use the wrapper with a test output root. This is the preferred test because it exercises the collector and formatter together.

```powershell
cd X:\PowerShell\Cohesity_API_Scripts\backup_failures

.\Cohesity_Backup_Failure_INC_Status_Update.ps1 `
  -IncidentNumber "INC999998" `
  -OutputRoot "X:\PowerShell\Data\Cohesity\BackupFailureWindow_WrapperTest" `
  -RequestTimeoutSec 120
```

Expected on first run for this test INC/window:

```text
RunMode = Baseline
Scan NumRuns = 30
Formatter runs after collector
```

## 5. Locate test output

```powershell
$latest = Get-ChildItem "X:\PowerShell\Data\Cohesity\BackupFailureWindow_WrapperTest" -Directory |
  Where-Object { $_.Name -ne "Archive" } |
  Sort-Object LastWriteTime -Descending |
  Select-Object -First 1

"Latest folder: $($latest.FullName)"
Get-ChildItem $latest.FullName
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
state.previous.json
no_object_evidence_review.csv
```

## 6. Validate worknotes

```powershell
Get-Content (Join-Path $latest.FullName "worknotes_summary.txt")
```

Expected content:

```text
Summary:
Active Failures: <count>
Recovered Today: <count>
Running / In-progress PGs: <count>
Cancelled Backup PGs: <count>

Failure Section:
...

Success Section:
...
```

If running or cancelled count is greater than zero, expected note:

```text
Please check incident_lifecycle.csv and continue monitoring running/cancelled backups.
```

Do not expect the sentence:

```text
These are not counted as failures or success.
```

That sentence was intentionally removed.

## 7. Validate operator-facing lifecycle columns

```powershell
Import-Csv (Join-Path $latest.FullName "incident_lifecycle.csv") |
  Select -First 1 |
  Get-Member -MemberType NoteProperty |
  Select -ExpandProperty Name
```

Expected columns:

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

These technical columns should not be in operator-facing incident_lifecycle.csv:

```text
WindowKey
ObjectKey
ClusterId
ProtectionGroupId
EnvironmentFilter
FailedRunKeys
```

They should remain available in:

```text
incident_lifecycle_raw.csv
state.json
```

## 8. Validate active failures

```powershell
Import-Csv (Join-Path $latest.FullName "current_failures.csv") |
  Select Status,StatusChange,Cluster,ProtectionGroup,Environment,Host,ObjectName,ObjectType,RunType,LastFailedET,FailureDates,ConsecutiveFailureDays,FailedRunCount,Message |
  Format-Table -AutoSize -Wrap
```

Pass condition:

```text
ObjectName is populated for real object failures.
ProtectionGroup is context only.
No fake PG-as-object failure row exists.
RemoteAdapter does not appear.
Same object failed multiple times in one day appears once with latest failure details.
```

## 9. Validate recovered objects

```powershell
Import-Csv (Join-Path $latest.FullName "cleared_by_success.csv") |
  Select Status,StatusChange,Cluster,ProtectionGroup,Environment,Host,ObjectName,ObjectType,LatestSuccessET,Message |
  Format-Table -AutoSize -Wrap
```

Pass condition:

```text
Only previously failed objects with newer same-object success are shown here.
```

## 10. Validate warnings

```powershell
Get-Content (Join-Path $latest.FullName "collection_warnings.txt")
```

Pass condition:

```text
Warnings are clean, or API errors are explicit.
If API errors occurred, worknotes/closing status must not look clean.
```

## 11. Incremental rerun test

Run the same wrapper test command again with the same test INC and same test output root.

```powershell
cd X:\PowerShell\Cohesity_API_Scripts\backup_failures

.\Cohesity_Backup_Failure_INC_Status_Update.ps1 `
  -IncidentNumber "INC999998" `
  -OutputRoot "X:\PowerShell\Data\Cohesity\BackupFailureWindow_WrapperTest" `
  -RequestTimeoutSec 120
```

Expected second run:

```text
RunMode = Incremental
Scan NumRuns = 15
Formatter runs after collector
```

Recheck files:

```powershell
$latest = Get-ChildItem "X:\PowerShell\Data\Cohesity\BackupFailureWindow_WrapperTest" -Directory |
  Where-Object { $_.Name -ne "Archive" } |
  Sort-Object LastWriteTime -Descending |
  Select-Object -First 1

Get-Content (Join-Path $latest.FullName "worknotes_summary.txt")
Import-Csv (Join-Path $latest.FullName "incident_lifecycle.csv") | Format-Table -AutoSize -Wrap
```

## 12. Normal production command

Only after test passes, use this as the normal daily command:

```powershell
cd X:\PowerShell\Cohesity_API_Scripts\backup_failures

.\Cohesity_Backup_Failure_INC_Status_Update.ps1
```

## Do not normally run direct collector

Do not use this for daily operation:

```powershell
.\Get-CohesityBackupFailureWindowConsolidator.ps1
```

Direct collector is only for debugging. If direct collector is used, the formatter must be run separately.

## Latest update - use this bottom section as the source of truth

```text
Normal daily use:
Run Cohesity_Backup_Failure_INC_Status_Update.ps1 only.

Safe test:
Run Cohesity_Backup_Failure_INC_Status_Update.ps1 with -IncidentNumber and -OutputRoot pointing to a test folder.

Do not follow older instructions that say to test direct collector first.
The wrapper is now the correct primary entry point because it runs both collector and formatter.

The main collector is still untested here against Helios.
The formatter is post-processing only and does not change collection/state logic.
```
