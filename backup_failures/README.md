# Backup Failures

Use this folder for the Cohesity backup failure incident workflow.

## Main PowerShell script

```powershell
backup_failures/Get-CohesityBackupFailureWindowConsolidator.ps1
```

## Test/run instructions

```powershell
backup_failures/RUNBOOK_Test_Instructions.md
```

## Window rule

The PowerShell script uses the same Dynatrace compute window logic:

```text
America/New_York
18:00 ET -> next day 18:00 ET
WindowKey: yyyy-MM-dd_1800ET
SNOW compare fields: snStartUtc / snEndUtc
```

The script does not ask for compute-window start or end time.
It asks only once for the incident number when a new DT window starts.

## API safety

Production Cohesity calls are GET-only.
The script uses:

```text
GET /v2/mcm/cluster-mgmt/info
GET /v2/data-protect/protection-groups
GET /v2/data-protect/protection-groups/{id}/runs
```

No POST, PUT, PATCH, or DELETE Cohesity API calls are used.

## Evidence output

The script now supports CSV fallback.

Preferred output when `ImportExcel` / `Export-Excel` exists:

```text
<INC>_BackupFailure_WindowSummary.xlsx
<INC>_WorkNotes_Paste.txt
<INC>_State.json
```

Fallback output when XLSX export is unavailable or `-ForceCsv` is used:

```text
<INC>_BackupFailure_CSV_Evidence\
<INC>_WorkNotes_Paste.txt
<INC>_State.json
```

The CSV evidence folder contains one CSV per workbook tab, including:

```text
00_Run_Status.csv
01_Summary.csv
02_Current_Still_Failing.csv
03_Recovered_In_Window.csv
04_New_Failures_Latest.csv
05_New_Recoveries_Latest.csv
06_Consecutive_Failures.csv
07_Carry_Forward_Baseline.csv
08_Event_History.csv
09_Run_Evidence.csv
10_Metadata.csv
11_Warnings.csv
00_Attach_These_CSV_Files.txt
```

No Microsoft Excel installation is required for CSV fallback.

## First test

```powershell
cd backup_failures
.\Get-CohesityBackupFailureWindowConsolidator.ps1 `
  -MaxClusters 1 `
  -MaxProtectionGroupsPerCluster 3 `
  -ShowGridView:$false `
  -ForceCsv
```

Run it twice. First run should ask for incident. Second run in the same DT window should not ask again.
