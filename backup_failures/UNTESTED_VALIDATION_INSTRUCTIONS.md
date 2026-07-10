# Cohesity Backup Failures - Untested Validation Instructions

## Important status

This code was pushed but has not been validated against Helios from ChatGPT.
Validation must be performed on the PowerShell host.

Do not treat the report as production-ready until these checks pass.

## Repository context

```text
Repo:   rupertrosario/my-redirect-page
Branch: Cohesity_Automations
Folder: backup_failures/
```

## Files changed

### Main collector

```text
backup_failures/Get-CohesityBackupFailureWindowConsolidator.ps1
```

Commit:

```text
1f70fba67e63ca8143d1f54efb1b209ca4067557
```

Purpose:

```text
Implemented daily baseline + incremental object-level backup failure tracking.
Baseline = 30 runs.
Incremental = 15 runs.
Object-level is source of truth.
RemoteAdapter excluded.
State backup / lock / atomic state write added.
Incomplete API collection marked as rerun required.
```

### Wrapper

```text
backup_failures/Cohesity_Backup_Failure_INC_Status_Update.ps1
```

Commits:

```text
15ebce303819998d8e6f2a0946eb5d6c66f0c539
8ac82207fbb8b401caa6fa9b01e981be4aefe3b7
```

Purpose:

```text
Default NumRuns changed from 20 to 15.
Wrapper text updated to say baseline 30 happens in collector.
Wrapper now runs the formatter after collector completes.
No collection logic belongs in the wrapper.
```

### Formatter

```text
backup_failures/Format-CohesityBackupFailureReport.ps1
```

Commit:

```text
7336fd59dddc53d0225523d9b85ebd8ac3c7369a
```

Purpose:

```text
Post-processing only.
Does not call Cohesity.
Does not change state.json.
Keeps raw collector CSVs as *_raw.csv.
Rewrites operator-facing CSVs with cleaner columns.
Renames Change to StatusChange.
Removes technical columns from operator-facing lifecycle CSV.
Rewrites worknotes_summary.txt with failures, recovered objects, and concise running/cancelled counts.
```

## Validation principle

Use a test output root first.
Do not start with the production output folder.

Test root:

```text
X:\PowerShell\Data\Cohesity\BackupFailureWindow_Test
```

Production root:

```text
X:\PowerShell\Data\Cohesity\BackupFailureWindow
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

Select-String .\Get-CohesityBackupFailureWindowConsolidator.ps1 -Pattern `
"BaselineNumRuns","RemoteAdapter","Final reporting is object-level only","Clear only when the same ObjectKey"

Select-String .\Cohesity_Backup_Failure_INC_Status_Update.ps1 -Pattern `
"Incremental NumRuns","Baseline NumRuns","Format-CohesityBackupFailureReport"

Select-String .\Format-CohesityBackupFailureReport.ps1 -Pattern `
"StatusChange","Running / In-progress PGs","Cancelled Backup PGs","incident_lifecycle_raw.csv"
```

Expected:

```text
Matches should appear from all three files.
```

If no matches appear, stop. You are not testing the latest pushed code.

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

If this fails, stop and fix syntax first.

## 4. Safe baseline test with direct collector plus formatter

Do not use the production folder yet.

```powershell
cd X:\PowerShell\Cohesity_API_Scripts\backup_failures

.\Get-CohesityBackupFailureWindowConsolidator.ps1 `
  -IncidentNumber "INC999999" `
  -OutputRoot "X:\PowerShell\Data\Cohesity\BackupFailureWindow_Test" `
  -RequestTimeoutSec 120
```

Expected first test behavior:

```text
RunMode = Baseline
Scan NumRuns = 30
```

Because this is a new test incident/state folder.

Now run the formatter manually for the direct collector test:

```powershell
$latest = Get-ChildItem "X:\PowerShell\Data\Cohesity\BackupFailureWindow_Test" -Directory |
  Where-Object { $_.Name -ne "Archive" } |
  Sort-Object LastWriteTime -Descending |
  Select-Object -First 1

.\Format-CohesityBackupFailureReport.ps1 -ReportFolder $latest.FullName
```

## 5. Locate latest test output

```powershell
"Latest folder: $($latest.FullName)"
Get-ChildItem $latest.FullName
```

Expected files should include most of these:

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

Empty CSVs with headers are acceptable if there are no matching rows.

## 6. Validate operator-facing lifecycle CSV columns

```powershell
Import-Csv (Join-Path $latest.FullName "incident_lifecycle.csv") |
  Select -First 1 |
  Get-Member -MemberType NoteProperty |
  Select -ExpandProperty Name
```

Expected operator-facing lifecycle columns:

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

These should not be present in operator-facing `incident_lifecycle.csv`:

```text
WindowKey
ObjectKey
ClusterId
ProtectionGroupId
EnvironmentFilter
FailedRunKeys
```

Those technical fields should be preserved in:

```text
incident_lifecycle_raw.csv
state.json
```

## 7. Validate current failures are object-level

```powershell
Import-Csv (Join-Path $latest.FullName "current_failures.csv") |
  Select Status,StatusChange,Cluster,ProtectionGroup,Environment,Host,ObjectName,ObjectType,RunType,LastFailedET,FailureDates,ConsecutiveFailureDays,FailedRunCount,Message |
  Format-Table -AutoSize -Wrap
```

Pass condition:

```text
ObjectName is populated for real object failures.
ProtectionGroup is context only.
No fake PG-as-object row exists.
RemoteAdapter does not appear.
Same object failed multiple times in one day appears once with latest failure details.
```

Fail condition:

```text
PG-only rows are shown as object failures.
ObjectName is blank for real object failures.
RemoteAdapter appears.
```

## 8. Validate cleared objects

```powershell
Import-Csv (Join-Path $latest.FullName "cleared_by_success.csv") |
  Select Status,StatusChange,Cluster,ProtectionGroup,Environment,Host,ObjectName,ObjectType,LatestSuccessET,Message |
  Format-Table -AutoSize -Wrap
```

Pass condition:

```text
Only previously failed objects with newer same ObjectKey success are shown here.
```

Fail condition:

```text
Objects are cleared because PG succeeded but same ObjectKey did not succeed.
```

## 9. Validate worknotes summary

```powershell
Get-Content (Join-Path $latest.FullName "worknotes_summary.txt")
```

Pass condition:

```text
Contains Summary.
Contains Active Failures count.
Contains Recovered Today count.
Contains Running / In-progress PGs count.
Contains Cancelled Backup PGs count.
Contains Failure Section.
Contains Success Section.
Does not list running/cancelled details in the failure table unless they are actual Failure status rows.
Does not contain the sentence: These are not counted as failures or success.
```

Expected running/cancelled note if count is greater than zero:

```text
Please check incident_lifecycle.csv and continue monitoring running/cancelled backups.
```

## 10. Validate collection warnings

```powershell
Get-Content (Join-Path $latest.FullName "collection_warnings.txt")
```

Pass condition:

```text
Warnings are empty/clean, or API errors are explicit.
If API errors occurred, report status must indicate Incomplete - RERUN REQUIRED.
```

Fail condition:

```text
API errors occurred but report looks clean.
```

## 11. Incremental rerun test

Run the same test command again with the same test INC and same test output root.

```powershell
cd X:\PowerShell\Cohesity_API_Scripts\backup_failures

.\Get-CohesityBackupFailureWindowConsolidator.ps1 `
  -IncidentNumber "INC999999" `
  -OutputRoot "X:\PowerShell\Data\Cohesity\BackupFailureWindow_Test" `
  -RequestTimeoutSec 120
```

Expected second test behavior:

```text
RunMode = Incremental
Scan NumRuns = 15
Same INC/state reused.
Known failed objects are carried forward if not seen.
Objects clear only with same ObjectKey newer success.
```

Run formatter again:

```powershell
$latest = Get-ChildItem "X:\PowerShell\Data\Cohesity\BackupFailureWindow_Test" -Directory |
  Where-Object { $_.Name -ne "Archive" } |
  Sort-Object LastWriteTime -Descending |
  Select-Object -First 1

.\Format-CohesityBackupFailureReport.ps1 -ReportFolder $latest.FullName
```

## 12. Recheck output after incremental rerun

```powershell
foreach ($file in @("current_failures.csv","cleared_by_success.csv","incident_lifecycle.csv","collection_warnings.txt","worknotes_summary.txt")) {
    "`n===== $file ====="
    $path = Join-Path $latest.FullName $file
    if (Test-Path $path) {
        if ($file -like "*.csv") {
            Import-Csv $path | Format-Table -AutoSize -Wrap
        } else {
            Get-Content $path
        }
    } else {
        "Missing: $path"
    }
}
```

## 13. Wrapper test after safe direct test passes

The wrapper now runs the formatter automatically.

Use a test output root first:

```powershell
cd X:\PowerShell\Cohesity_API_Scripts\backup_failures

.\Cohesity_Backup_Failure_INC_Status_Update.ps1 `
  -IncidentNumber "INC999998" `
  -OutputRoot "X:\PowerShell\Data\Cohesity\BackupFailureWindow_WrapperTest" `
  -RequestTimeoutSec 120
```

Expected:

```text
Collector runs.
Formatter runs.
Final operator-facing files are listed.
Raw CSV copies are listed for troubleshooting.
```

## 14. Only after safe tests pass, run normal production wrapper

```powershell
cd X:\PowerShell\Cohesity_API_Scripts\backup_failures

.\Cohesity_Backup_Failure_INC_Status_Update.ps1
```

Then validate production output:

```powershell
$latest = Get-ChildItem "X:\PowerShell\Data\Cohesity\BackupFailureWindow" -Directory |
  Where-Object { $_.Name -ne "Archive" } |
  Sort-Object LastWriteTime -Descending |
  Select-Object -First 1

"Latest folder: $($latest.FullName)"
Get-ChildItem $latest.FullName
Get-Content (Join-Path $latest.FullName "worknotes_summary.txt")
```

## Final pass / fail rule

PASS:

```text
Baseline first test uses 30.
Same INC rerun uses 15.
current_failures.csv is object-level.
cleared_by_success.csv only has same-object newer success.
incident_lifecycle.csv uses StatusChange and hides technical IDs.
incident_lifecycle_raw.csv preserves technical IDs.
worknotes_summary.txt shows only summary, failures, success/recovered, running count, cancelled count, and collection status.
RemoteAdapter is excluded.
Warnings are explicit.
Incomplete collection is not presented as clean.
```

FAIL:

```text
Syntax error.
PG-only rows shown as object failures.
Object failures missing while diagnostic sees them and they are not cleared.
API errors happen but report still looks clean.
RemoteAdapter appears.
Same-object success is not required for clearing.
Work notes contain unnecessary lifecycle/debug columns.
Work notes include running/cancelled rows as normal failure details.
```

## Do not do this until validation passes

```text
Do not mark production-ready.
Do not rely on worknotes_summary.txt for live incident updates.
Do not push further functional changes unless a failed validation case is understood.
```
