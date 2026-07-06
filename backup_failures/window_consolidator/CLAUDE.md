# Claude Guardrails — Backup Failure Window Consolidator

## Non-negotiable Rules

1. Do not use Excel, XLSX, COM objects, ImportExcel, or workbook tabs.
2. Output evidence as CSV, TXT, and JSON only.
3. Production Cohesity behavior must remain GET-only.
4. Do not add POST, PUT, PATCH, or DELETE calls to Cohesity.
5. Do not write to ServiceNow from this script.
6. Do not ask for the incident number again inside the same compute window.
7. Do not overwrite an existing incident/window registry mapping unless an explicit future override switch is added.
8. Keep `compute_window.js` as the incident-window source of truth.
9. Keep window semantics aligned to daily 18:00 ET -> next-day 18:00 ET.
10. Keep console output short. Detailed evidence belongs in CSV/TXT/JSON.

## Reference Files

Use these as implementation references:

```text
backup_failures/window_consolidator/compute_window.js
backup_failures/window_consolidator/Get-CohesityBackupFailureWindowConsolidator.ps1
```

Older Dynatrace scripts in the repo show the GET-only Helios pattern and object/run expansion logic. Preserve that style.

## Required Operator Behavior

First run in new window:

```text
Prompt for INC number once.
Lock INC to WindowKey.
Write registry and state.
Generate CSV/TXT/JSON.
```

Later runs in same window:

```text
Reuse locked INC.
Do not prompt.
Compare current result with previous incident state.
Generate new failures and new recoveries.
```

New compute window:

```text
Prompt for new INC.
Carry forward previous still-failing baseline.
Do not mix incidents.
```

## Required Output Files

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
QuickView.csv
WorkNotes_Paste.txt
State.json
BackupFailure_WindowRegistry.json
```

## Validation Command

```powershell
.\Get-CohesityBackupFailureWindowConsolidator.ps1 `
  -MaxClusters 1 `
  -MaxProtectionGroupsPerCluster 3 `
  -NoGridView
```

Expected:

```text
Prompts once for incident on first run.
Does not prompt on second run in same window.
Creates CSV/TXT/JSON only.
No XLSX file appears.
```

## Do Not Change Without Explicit Approval

```text
Helios Base URL: https://helios.cohesity.com
API key path: X:\PowerShell\Cohesity_API_Scripts\DO_NOT_Delete\apikey.txt
Output root: X:\PowerShell\Data\Cohesity\BackupFailureWindow
Time zone: America/New_York
```
