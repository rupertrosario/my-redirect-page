# FAST TEST RUN GUIDE - Backup Failure consolidator

Use this when the full all-cluster run is taking too long.

## Why it is slow

The full run scans:

```text
all environments -> all clusters -> all protection groups -> detailed runs/objects
```

For a new window or new incident baseline, the consolidator uses 30 runs. Across 23 clusters this can take a long time.

## If the current run is still running

Do not pull GitHub changes while the PowerShell run is active.

If this is only a test run and it is taking too long, stop it with:

```powershell
Ctrl+C
```

After the PowerShell prompt returns, remove the collector lock only after confirming no collector process is still running:

```powershell
$latest = Get-ChildItem "X:\PowerShell\Data\Cohesity\BackupFailureWindow_Test" -Directory |
  Where-Object { $_.Name -ne "Archive" } |
  Sort-Object LastWriteTime -Descending |
  Select-Object -First 1

$lock = Join-Path $latest.FullName "collector.lock"
if (Test-Path $lock) {
  Remove-Item $lock -Force
  "Removed stale collector lock: $lock"
}
```

## Fast one-cluster test

Use one cluster first. This validates the logic without scanning everything.

```powershell
cd X:\PowerShell\Cohesity_API_Scripts\backup_failures

.\Cohesity_Backup_Failure_INC_Status_Update.ps1 `
  -IncidentNumber "INC999998" `
  -OutputRoot "X:\PowerShell\Data\Cohesity\BackupFailureWindow_Test" `
  -ClusterName "<EXACT_CLUSTER_NAME>" `
  -NumRuns 3 `
  -RequestTimeoutSec 60
```

## If testing the consolidator directly

The wrapper currently does not expose BaselineNumRuns. For fastest testing, run the consolidator directly with a smaller baseline and then run the formatter.

```powershell
cd X:\PowerShell\Cohesity_API_Scripts\backup_failures

.\Get-CohesityBackupFailureWindowConsolidator.ps1 `
  -IncidentNumber "INC999998" `
  -OutputRoot "X:\PowerShell\Data\Cohesity\BackupFailureWindow_Test" `
  -ClusterName "<EXACT_CLUSTER_NAME>" `
  -NumRuns 3 `
  -BaselineNumRuns 5 `
  -RequestTimeoutSec 60

$latest = Get-ChildItem "X:\PowerShell\Data\Cohesity\BackupFailureWindow_Test" -Directory |
  Where-Object { $_.Name -ne "Archive" } |
  Sort-Object LastWriteTime -Descending |
  Select-Object -First 1

.\Format-CohesityBackupFailureReport.ps1 -ReportFolder $latest.FullName
```

## Full production-style run

Use this only after the one-cluster test output looks correct.

```powershell
cd X:\PowerShell\Cohesity_API_Scripts\backup_failures

.\Cohesity_Backup_Failure_INC_Status_Update.ps1 `
  -IncidentNumber "INC999998" `
  -OutputRoot "X:\PowerShell\Data\Cohesity\BackupFailureWindow_Test" `
  -NumRuns 15 `
  -RequestTimeoutSec 120
```

## Expected speed behavior

```text
One cluster + NumRuns 3/5 = fast validation
All clusters + baseline 30 = slow but complete
```
