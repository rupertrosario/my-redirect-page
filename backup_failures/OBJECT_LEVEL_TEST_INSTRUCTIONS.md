# Cohesity Backup Failure Object-Level Validation

Use this runbook when `Test-CohesityRunObjectDetails.ps1` shows failed objects but `incident_lifecycle.csv` or Excel shows only the Protection Group.

## Rule being validated

If a failed run has objects, the collector must output object rows.
Only if a failed run has zero objects should the collector output a PG/run-level fallback row.

## Files in scope

Update only this file:

```text
backup_failures/Get-CohesityBackupFailureWindowConsolidator.ps1
```

Do not change this wrapper unless explicitly required:

```text
backup_failures/Cohesity_Backup_Failure_INC_Status_Update.ps1
```

The wrapper should continue calling the collector normally.

## 1. Pull latest branch

```powershell
cd X:\PowerShell\Cohesity_API_Scripts
git fetch origin
git reset --hard origin/Cohesity_Automations
```

## 2. Replace the collector file

Replace this local file with the corrected full script:

```text
X:\PowerShell\Cohesity_API_Scripts\backup_failures\Get-CohesityBackupFailureWindowConsolidator.ps1
```

Do not run `git reset --hard` after replacing it.

## 3. Confirm you are running the corrected collector

```powershell
cd X:\PowerShell\Cohesity_API_Scripts\backup_failures

Select-String .\Get-CohesityBackupFailureWindowConsolidator.ps1 -Pattern `
"Object-selection model","objectsToWrite","reviewObjects","Run marked Failed; Cohesity returned this object"
```

Expected: matches are returned.

Expected example:

```text
Get-CohesityBackupFailureWindowConsolidator.ps1:10:Object-selection model:
Get-CohesityBackupFailureWindowConsolidator.ps1:650:                        $reviewObjects = @()
Get-CohesityBackupFailureWindowConsolidator.ps1:652:                            $reviewObjects = @($objectsAll)
Get-CohesityBackupFailureWindowConsolidator.ps1:655:                        $objectsToWrite = @($candidateObjects + $reviewObjects)
Get-CohesityBackupFailureWindowConsolidator.ps1:672:                                    $msg = "Run marked Failed; Cohesity returned this object without explicit failedAttempts/status/error evidence"
```

If no matches are returned, you are still running the old collector.

## 4. Validate syntax

```powershell
cd X:\PowerShell\Cohesity_API_Scripts

$file = ".\backup_failures\Get-CohesityBackupFailureWindowConsolidator.ps1"
[scriptblock]::Create((Get-Content $file -Raw)) | Out-Null
(Get-Content $file).Count
```

Expected: line count is around 1000+ lines. It must not be 229.

## 5. Run diagnostic for the same PG

Use the PG where Cohesity UI or previous test showed failed objects.

```powershell
cd X:\PowerShell\Cohesity_API_Scripts\backup_failures

.\Test-CohesityRunObjectDetails.ps1 `
  -ClusterName "YOUR_CLUSTER_NAME" `
  -ProtectionGroupName "YOUR_PG_NAME" `
  -NumRuns 10
```

Expected: the diagnostic should show object rows and `Rows with failures` should match the failed object count.

## 6. Run a clean object-level collector test

This avoids old `state.json` or previous INC folder rows carrying forward a PG-only fallback.

```powershell
cd X:\PowerShell\Cohesity_API_Scripts\backup_failures

.\Get-CohesityBackupFailureWindowConsolidator.ps1 `
  -ClusterName "YOUR_CLUSTER_NAME" `
  -IncidentNumber "INC999999" `
  -OutputRoot "X:\PowerShell\Data\Cohesity\BackupFailureWindow_ObjectTest" `
  -RequestTimeoutSec 45
```

## 7. Check object rows in clean collector output

```powershell
$latest = Get-ChildItem "X:\PowerShell\Data\Cohesity\BackupFailureWindow_ObjectTest" -Directory |
  Where-Object { $_.Name -ne "Archive" } |
  Sort-Object LastWriteTime -Descending |
  Select-Object -First 1

"Latest folder: $($latest.FullName)"

foreach ($file in @("current_failures.csv","incident_lifecycle_raw.csv","incident_lifecycle.csv","cleared_by_success.csv")) {
    "`n===== $file ====="
    $path = Join-Path $latest.FullName $file
    if (Test-Path $path) {
        Import-Csv $path |
          Select Cluster,ProtectionGroup,Environment,Host,ObjectName,ObjectType,Status,Message |
          Format-Table -AutoSize -Wrap
    } else {
        "Missing: $path"
    }
}
```

Expected for the PG where diagnostic shows 3 failed objects:

```text
ProtectionGroup    ObjectName        ObjectType       Status
PG_NAME            object-1          ...              NewlyFailedThisCheck / UnknownNeedsReview
PG_NAME            object-2          ...              NewlyFailedThisCheck / UnknownNeedsReview
PG_NAME            object-3          ...              NewlyFailedThisCheck / UnknownNeedsReview
```

If objects are active failures, they should be in:

```text
current_failures.csv
incident_lifecycle.csv
```

If they were cleared by later success, they should be in:

```text
cleared_by_success.csv
```

## 8. If object rows are still missing, run deterministic object-name search

This searches the exact failed object names from the diagnostic CSV across every collector CSV.

### 8.1 Get latest diagnostic failed object names

```powershell
$debugCsv = Get-ChildItem "X:\PowerShell\Data\Cohesity\BackupFailureWindow\Debug" -Filter "Cohesity_RunObjectDetails_*.csv" |
  Sort-Object LastWriteTime -Descending |
  Select-Object -First 1

"Debug CSV: $($debugCsv.FullName)"

$debugRows = Import-Csv $debugCsv.FullName
$failedObjects = @($debugRows |
  Where-Object { [int]$_.FailedAttemptsCount -gt 0 } |
  Select-Object -ExpandProperty ObjectName -Unique)

"`nFAILED OBJECT NAMES FROM DIAGNOSTIC:"
$failedObjects
```

Expected: the failed object names are printed.

### 8.2 Search those exact object names in clean collector output

```powershell
$collectorRoot = "X:\PowerShell\Data\Cohesity\BackupFailureWindow_ObjectTest"

$latestCollector = Get-ChildItem $collectorRoot -Directory |
  Where-Object { $_.Name -ne "Archive" } |
  Sort-Object LastWriteTime -Descending |
  Select-Object -First 1

"Collector folder: $($latestCollector.FullName)"

$collectorFiles = @(
  "current_failures.csv",
  "incident_lifecycle_raw.csv",
  "incident_lifecycle.csv",
  "cleared_by_success.csv"
)

foreach ($file in $collectorFiles) {
    "`n===== SEARCHING $file ====="
    $path = Join-Path $latestCollector.FullName $file

    if (!(Test-Path $path)) {
        "Missing: $path"
        continue
    }

    $rows = Import-Csv $path

    foreach ($obj in $failedObjects) {
        $matches = @($rows | Where-Object { $_.ObjectName -eq $obj })

        if ($matches.Count -gt 0) {
            "FOUND: $obj in $file"
            $matches |
              Select Cluster,ProtectionGroup,Environment,Host,ObjectName,ObjectType,Status,Message |
              Format-Table -AutoSize -Wrap
        } else {
            "NOT FOUND: $obj in $file"
        }
    }
}
```

### 8.3 Search by Protection Group in clean collector output

Replace `YOUR_PG_NAME` with the exact PG name from diagnostic.

```powershell
$pgName = "YOUR_PG_NAME"

foreach ($file in $collectorFiles) {
    "`n===== PG SEARCH: $file ====="
    $path = Join-Path $latestCollector.FullName $file

    if (!(Test-Path $path)) {
        "Missing: $path"
        continue
    }

    Import-Csv $path |
      Where-Object { $_.ProtectionGroup -eq $pgName -or $_.ProtectionGroup -like "*$pgName*" } |
      Select Cluster,ProtectionGroup,Environment,Host,ObjectName,ObjectType,Status,Message |
      Format-Table -AutoSize -Wrap
}
```

### 8.4 Interpret result

```text
If objects are in cleared_by_success.csv:
They were suppressed because the collector detected later success.

If objects are nowhere, but PG row exists:
The collector is not processing the same object rows as diagnostic.

If PG is not found either:
The clean collector test is not targeting the same cluster/PG/run as diagnostic.
```

## 9. If clean test works, run normal wrapper

```powershell
cd X:\PowerShell\Cohesity_API_Scripts\backup_failures

.\Cohesity_Backup_Failure_INC_Status_Update.ps1 `
  -ClusterName "YOUR_CLUSTER_NAME" `
  -RequestTimeoutSec 45
```

Then check the normal output folder:

```powershell
$latest = Get-ChildItem "X:\PowerShell\Data\Cohesity\BackupFailureWindow" -Directory |
  Where-Object { $_.Name -ne "Archive" } |
  Sort-Object LastWriteTime -Descending |
  Select-Object -First 1

Import-Csv (Join-Path $latest.FullName "incident_lifecycle.csv") |
  Where-Object { $_.ProtectionGroup -like "*YOUR_PG_NAME*" } |
  Select Cluster,ProtectionGroup,Environment,Host,ObjectName,ObjectType,Status,Message |
  Format-Table -AutoSize -Wrap
```

## 10. Commit only the collector change after validation

```powershell
cd X:\PowerShell\Cohesity_API_Scripts

git status --short
git diff -- backup_failures/Get-CohesityBackupFailureWindowConsolidator.ps1

git add backup_failures/Get-CohesityBackupFailureWindowConsolidator.ps1
git commit -m "Fix backup failure object-level output"
git push origin Cohesity_Automations
```

## Pass / fail rule

```text
PASS:
Diagnostic shows failed objects, and collector shows those same objects either active or cleared.

FAIL:
Diagnostic shows failed objects, but collector output still shows only PG-level row and no object rows anywhere.
```
