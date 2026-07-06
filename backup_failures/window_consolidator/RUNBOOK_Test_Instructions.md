# Backup Failure Window Consolidator - Test Runbook

Use this on the machine where PowerShell, Git, network access, and the Cohesity API key are available.

## 1. Open PowerShell

Open normal Windows PowerShell or PowerShell 7.

## 2. Go to your local Git repo

Replace the path below with your local clone path.

```powershell
cd C:\Path\To\my-redirect-page
```

## 3. Pull the latest code

```powershell
git checkout Cohesity_Automations
git pull
```

## 4. Go to the script folder

```powershell
cd backup_failures\window_consolidator
```

## 5. Confirm the API key file exists

```powershell
Test-Path "X:\PowerShell\Cohesity_API_Scripts\DO_NOT_Delete\apikey.txt"
```

Expected:

```text
True
```

If it returns `False`, fix the API key file path before running the script.

## 6. Optional: install XLSX export module

Run this only if `ImportExcel` is not already installed and internet/module repository access is allowed from the machine.

```powershell
Install-Module ImportExcel -Scope CurrentUser
```

If this cannot be installed, the script will try Excel COM automation if Microsoft Excel is installed.

## 7. First limited test run

Run this first. Do not start with all clusters.

```powershell
.\Get-CohesityBackupFailureWindowConsolidator.ps1 `
  -MaxClusters 1 `
  -MaxProtectionGroupsPerCluster 3 `
  -ShowGridView:$false
```

What happens immediately:

1. Script calculates the current Dynatrace compute window: `18:00 ET -> next day 18:00 ET`.
2. Script builds a window key like `yyyy-MM-dd_1800ET`.
3. Script checks `X:\PowerShell\Data\Cohesity\BackupFailureWindow\BackupFailure_WindowRegistry.json`.
4. If the window is new, it asks once: `Enter incident number for this window:`.
5. Enter the incident number, for example `INC1234567`.
6. Script locks that incident to the DT compute window.
7. Script collects Helios data using GET only.
8. Script creates the output files.

## 8. Second limited test run

Run the same command again:

```powershell
.\Get-CohesityBackupFailureWindowConsolidator.ps1 `
  -MaxClusters 1 `
  -MaxProtectionGroupsPerCluster 3 `
  -ShowGridView:$false
```

Expected result:

```text
It must NOT ask for the incident number again.
```

If it asks again inside the same DT window, stop and review the registry file.

## 9. Expected output folder

The files are created under:

```text
X:\PowerShell\Data\Cohesity\BackupFailureWindow\INC1234567\
```

Expected files:

```text
INC1234567_BackupFailure_WindowSummary.xlsx
INC1234567_WorkNotes_Paste.txt
INC1234567_State.json
```

Registry file:

```text
X:\PowerShell\Data\Cohesity\BackupFailureWindow\BackupFailure_WindowRegistry.json
```

## 10. Expected console output

You should see:

```text
Incident: INC1234567
Window  : yyyy-MM-dd 18:00 ET -> yyyy-MM-dd 18:00 ET

Summary:
Total Failed In Window
Recovered In Window
Still Failing Now
New Failures Since Last Run
New Recoveries Since Last Run
Consecutive Failures
Running Runs Seen
Cancelled Runs Seen

Files Created:
...
Next Step: Attach XLSX to incident and paste WorkNotes_Paste.txt into work_notes.
```

## 11. Validate the registry lock

Open:

```text
X:\PowerShell\Data\Cohesity\BackupFailureWindow\BackupFailure_WindowRegistry.json
```

Confirm the current window has:

```json
"WindowKey": "yyyy-MM-dd_1800ET",
"WindowLocked": true,
"WindowSource": "Dynatrace_compute_window",
"IncidentNumber": "INC1234567"
```

## 12. Validate the ServiceNow work notes

Open:

```text
X:\PowerShell\Data\Cohesity\BackupFailureWindow\INC1234567\INC1234567_WorkNotes_Paste.txt
```

Confirm it contains:

```text
Locked Compute Window
SNOW Compare UTC
Summary
Attachment
```

Paste this text into ServiceNow `work_notes`.

## 13. Full run after limited test passes

Only after the limited test passes twice, run full scope:

```powershell
.\Get-CohesityBackupFailureWindowConsolidator.ps1
```

Or full scope without GridView:

```powershell
.\Get-CohesityBackupFailureWindowConsolidator.ps1 -ShowGridView:$false
```

## 14. If the script fails

Copy the full red error text and the last console lines.

Also check these files if they exist:

```text
X:\PowerShell\Data\Cohesity\BackupFailureWindow\BackupFailure_WindowRegistry.json
X:\PowerShell\Data\Cohesity\BackupFailureWindow\<INC>\<INC>_State.json
```

Do not delete the registry unless you are intentionally resetting the locked window mapping.
