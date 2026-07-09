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

If no matches are returned, you are still running the old collector.

## 4. Validate syntax

```powershell
cd X:\PowerShell\Cohesity_API_Scripts

$file = ".\backup_failures\Get-CohesityBackupFailureWindowConsolidator.ps1"
[scriptblock]::Create((Get-Content $file -Raw)) | Out-Null
(Get-Content $file).Count
```

Expected: line count is around 1000+ lines. It must not be 229.

## 5. Run a clean object-level test

This avoids old `state.json` or previous INC folder rows carrying forward a PG-only fallback.

```powershell
cd X:\PowerShell\Cohesity_API_Scripts\backup_failures

.\Get-CohesityBackupFailureWindowConsolidator.ps1 `
  -ClusterName "YOUR_CLUSTER_NAME" `
  -IncidentNumber "INC999999" `
  -OutputRoot "X:\PowerShell\Data\Cohesity\BackupFailureWindow_ObjectTest" `
  -RequestTimeoutSec 45
```

## 6. Check object rows in clean output

```powershell
$latest = Get-ChildItem "X:\PowerShell\Data\Cohesity\BackupFailureWindow_ObjectTest" -Directory |
  Where-Object { $_.Name -ne "Archive" } |
  Sort-Object LastWriteTime -Descending |
  Select-Object -First 1

"Latest folder: $($latest.FullName)"

foreach ($file in @("current_failures.csv","incident_lifecycle_raw.csv","incident_lifecycle.csv")) {
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

## 7. If clean test works, run normal wrapper

```powershell
cd X:\PowerShell\Cohesity_API_Scripts\backup_failures

.\Cohesity_Backup_Failure_INC_Status_Update.ps1 `
  -ClusterName "YOUR_CLUSTER_NAME" `
  -RequestTimeoutSec 45
```

## 8. Commit only the collector change

```powershell
cd X:\PowerShell\Cohesity_API_Scripts

git status --short
git diff -- backup_failures/Get-CohesityBackupFailureWindowConsolidator.ps1

git add backup_failures/Get-CohesityBackupFailureWindowConsolidator.ps1
git commit -m "Fix backup failure object-level output"
git push origin Cohesity_Automations
```

## If object rows are still missing

Run the diagnostic again for the same PG:

```powershell
cd X:\PowerShell\Cohesity_API_Scripts\backup_failures
.\Test-CohesityRunObjectDetails.ps1
```

If diagnostic shows 3 failed objects but clean collector output still shows only the PG, compare:

```text
current_failures.csv
incident_lifecycle_raw.csv
incident_lifecycle.csv
```

The object must first appear in `current_failures.csv`. If it does not, the object-selection block is still not using the same evidence that the diagnostic script sees.
