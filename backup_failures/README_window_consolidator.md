# Backup Failure Window Consolidator

## Decision

Run the consolidator from this folder:

```powershell
cd .\backup_failures
.\Get-CohesityBackupFailureWindowConsolidator.ps1
```

The actual implementation is under:

```text
backup_failures/window_consolidator/Get-CohesityBackupFailureWindowConsolidator.ps1
```

The root script is only a launcher so the operator can run it from the existing `backup_failures` folder.

## Window Source

Use this existing file as the Dynatrace compute-window source of truth:

```text
backup_failures/compute_window.js
```

Current logic:

```text
18:00 ET -> next-day 18:00 ET
WindowKey: yyyy-MM-dd_1800ET
CorrelationId: Cohesity_Backup_Failures
```

## Incident Lock Rule

```text
Same compute window = same incident number
New compute window = ask for new incident number
```

The script writes and reuses:

```text
X:\PowerShell\Data\Cohesity\BackupFailureWindow\BackupFailure_WindowRegistry.json
```

## Outputs

CSV/TXT/JSON only. No Excel.

Per incident folder:

```text
X:\PowerShell\Data\Cohesity\BackupFailureWindow\INCxxxxxxx\
```

Main files:

```text
INCxxxxxxx_01_Summary.csv
INCxxxxxxx_02_Current_Still_Failing.csv
INCxxxxxxx_03_Recovered_In_Window.csv
INCxxxxxxx_04_New_Failures_Latest.csv
INCxxxxxxx_05_New_Recoveries_Latest.csv
INCxxxxxxx_06_Consecutive_Failures.csv
INCxxxxxxx_07_Carry_Forward_Baseline.csv
INCxxxxxxx_08_Event_History.csv
INCxxxxxxx_09_Run_Evidence.csv
INCxxxxxxx_QuickView.csv
INCxxxxxxx_WorkNotes_Paste.txt
INCxxxxxxx_State.json
```

## First Validation

```powershell
.\Get-CohesityBackupFailureWindowConsolidator.ps1 `
  -MaxClusters 1 `
  -MaxProtectionGroupsPerCluster 3 `
  -NoGridView
```

Expected:

```text
First run asks for incident once.
Second run in same 18:00 ET window does not ask again.
No XLSX is created.
```

## Production Rule

Cohesity calls are GET-only:

```text
GET /v2/mcm/cluster-mgmt/info
GET /v2/data-protect/protection-groups
GET /v2/data-protect/protection-groups/{pgId}/runs
```
