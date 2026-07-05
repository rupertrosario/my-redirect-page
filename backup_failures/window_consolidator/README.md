# Cohesity Backup Failure Window Consolidator

Operator-focused backup failure lifecycle tracker for Cohesity Helios.

This is **not** a Cohesity UI/report duplicate. The purpose is to remove manual consolidation from the team during backup-failure incident handling.

## What this script does

The script locks one ServiceNow incident to one compute window and then tracks:

- failures inside that locked window
- objects recovered by later successful backup inside the same window
- objects still failing at latest check
- new failures since the previous script run
- new recoveries since the previous script run
- consecutive/repeated failures
- running runs seen in the window
- cancelled runs seen in the window
- carry-forward baseline for the next incident/window

The team should only need to:

1. Run PowerShell.
2. Review the GridView if needed.
3. Attach the generated XLSX to the incident.
4. Paste the generated TXT into work_notes.
5. Troubleshoot the actual failures.

## File

```powershell
backup_failures/window_consolidator/Get-CohesityBackupFailureWindowConsolidator.ps1
```

## Default behavior

Default window model:

```text
6-hour ET windows aligned to 00/06/12/18
```

Default API key path:

```text
X:\PowerShell\Cohesity_API_Scripts\DO_NOT_Delete\apikey.txt
```

Default output root:

```text
X:\PowerShell\Data\Cohesity\BackupFailureWindow
```

## First run in a new compute window

Run:

```powershell
.\Get-CohesityBackupFailureWindowConsolidator.ps1
```

If the current compute window is not mapped yet, the script prompts once:

```text
Enter incident number for this window:
```

After the incident is entered, the script locks the incident to that compute window in:

```text
BackupFailure_WindowRegistry.json
```

## Later runs inside the same compute window

Run the same command:

```powershell
.\Get-CohesityBackupFailureWindowConsolidator.ps1
```

The script reuses the locked incident automatically. It does not prompt again.

## New compute window

When a new compute window starts, the script detects that a new incident mapping is needed and prompts once for the new incident number.

This prevents the team from mixing failure evidence across incidents/windows.

## Optional manual review window

Use this only when reviewing a specific historical window:

```powershell
.\Get-CohesityBackupFailureWindowConsolidator.ps1 `
  -IncidentNumber "INC1234567" `
  -WindowStartET "2026-07-05 18:00:00" `
  -WindowEndET   "2026-07-06 00:00:00"
```

## Outputs per incident

Output folder:

```text
X:\PowerShell\Data\Cohesity\BackupFailureWindow\INC1234567
```

Files:

```text
INC1234567_BackupFailure_WindowSummary.xlsx
INC1234567_WorkNotes_Paste.txt
INC1234567_State.json
```

## XLSX workbook tabs

| Tab | Purpose |
|---|---|
| `00_Run_Status` | Script result, window, incident, warnings |
| `01_Summary` | Window-level counts |
| `02_Current_Still_Failing` | Objects still failing at latest check |
| `03_Recovered_In_Window` | Objects that failed and later succeeded in the window |
| `04_New_Failures_Latest` | New failures compared with previous script run |
| `05_New_Recoveries_Latest` | New recoveries compared with previous script run |
| `06_Consecutive_Failures` | Objects failing across repeated schedules/checks |
| `07_Carry_Forward_Baseline` | Still-failing objects to use as next-window baseline |
| `08_Event_History` | Failure/recovery/running/cancelled event history |
| `09_Run_Evidence` | Run-level evidence from Cohesity API |
| `10_Metadata` | Script/API settings used |
| `11_Warnings` | Any collection or truncation warnings |

## GridView behavior

Default behavior opens **one consolidated GridView**:

```text
INC1234567 - Backup Failure Window Quick View
```

The GridView includes a `Section` column so operators can filter:

- Current Still Failing
- Recovered In Window
- New Failure
- New Recovery
- Consecutive Failure
- Running Run
- Cancelled Run

Optional separate grids:

```powershell
.\Get-CohesityBackupFailureWindowConsolidator.ps1 -MultipleGridViews
```

Disable GridView:

```powershell
.\Get-CohesityBackupFailureWindowConsolidator.ps1 -ShowGridView:$false
```

## Work notes output

The text file is paste-ready for SNOW work_notes:

```text
INC1234567_WorkNotes_Paste.txt
```

It summarizes:

- total failed in window
- recovered in window
- still failing now
- new failures
- new recoveries
- consecutive/repeated failures
- running/cancelled runs seen
- workbook attachment name

## Important operating principle

Script success does **not** mean backups succeeded.

Script success means:

```text
The script collected and consolidated the locked incident window successfully.
```

Backup lifecycle result is shown separately:

```text
Still failing / recovered / running / cancelled / consecutive failures
```

## XLSX dependency

The script exports XLSX by using either:

1. `ImportExcel` PowerShell module, if installed, or
2. Microsoft Excel COM automation, if Excel is installed.

If neither exists, XLSX export will fail and the script will state that clearly.

Recommended module:

```powershell
Install-Module ImportExcel -Scope CurrentUser
```

## Future SNOW work_notes readiness

Current design:

```text
Manual incident number only when a new window starts.
```

Future design if work_notes read is enabled:

```text
Read incident/work_notes -> extract compute window -> validate locked mapping -> generate workbook/TXT automatically.
```

The current JSON state and window registry are structured so that SNOW read can be added later without changing the operator workflow.
