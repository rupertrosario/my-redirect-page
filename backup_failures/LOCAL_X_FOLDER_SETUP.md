# Local X: Drive Setup - Backup Failure Window

This is the intended setup for the machine where you actually run PowerShell.

Do not depend on Git folder layout at runtime.
Use one local folder under `X:\PowerShell\Cohesity_API_Scripts`.

## 1. Create local folder

Create this folder on the PowerShell machine:

```text
X:\PowerShell\Cohesity_API_Scripts\BackupFailureWindow
```

## 2. Required local files

Copy these two files into that folder:

```text
X:\PowerShell\Cohesity_API_Scripts\BackupFailureWindow\Get-CohesityBackupFailureWindowConsolidator.ps1
X:\PowerShell\Cohesity_API_Scripts\BackupFailureWindow\RUN_ME.ps1
```

In Git, the runner file is named:

```text
backup_failures/RUN_ME_FROM_X_FOLDER.ps1
```

Copy it locally and rename it to:

```text
RUN_ME.ps1
```

## 3. API key location

The API key must remain here:

```text
X:\PowerShell\Cohesity_API_Scripts\DO_NOT_Delete\apikey.txt
```

The runner checks this file before starting.

## 4. Run command

From any PowerShell prompt, run:

```powershell
X:\PowerShell\Cohesity_API_Scripts\BackupFailureWindow\RUN_ME.ps1
```

This runs safe test mode by default:

```text
MaxClusters: 1
MaxProtectionGroupsPerCluster: 3
GridView: disabled
Evidence format: CSV
```

## 5. Full run command

After the safe test works twice, run:

```powershell
X:\PowerShell\Cohesity_API_Scripts\BackupFailureWindow\RUN_ME.ps1 -Full
```

## 6. Output location

All output is saved under:

```text
X:\PowerShell\Cohesity_API_Scripts\BackupFailureWindow\Output
```

Expected output layout:

```text
X:\PowerShell\Cohesity_API_Scripts\BackupFailureWindow\Output
├── BackupFailure_WindowRegistry.json
└── INCxxxxxxx
    ├── INCxxxxxxx_BackupFailure_CSV_Evidence
    │   ├── 00_Run_Status.csv
    │   ├── 01_Summary.csv
    │   ├── 02_Current_Still_Failing.csv
    │   ├── 03_Recovered_In_Window.csv
    │   ├── 04_New_Failures_Latest.csv
    │   ├── 05_New_Recoveries_Latest.csv
    │   ├── 06_Consecutive_Failures.csv
    │   ├── 07_Carry_Forward_Baseline.csv
    │   ├── 08_Event_History.csv
    │   ├── 09_Run_Evidence.csv
    │   ├── 10_Metadata.csv
    │   ├── 11_Warnings.csv
    │   └── 00_Attach_These_CSV_Files.txt
    ├── INCxxxxxxx_WorkNotes_Paste.txt
    └── INCxxxxxxx_State.json
```

## 7. What RUN_ME.ps1 prints before running

The runner prints:

```text
Running from
Main script
API key path
Output root
Registry file
Cohesity API mode: GET only
Evidence format: CSV
```

Use this to verify exactly what is running and where files are saved.

## 8. GET-only guarantee

The production Cohesity API calls are GET-only.

The script uses:

```text
GET /v2/mcm/cluster-mgmt/info
GET /v2/data-protect/protection-groups
GET /v2/data-protect/protection-groups/{id}/runs
```

No POST, PUT, PATCH, or DELETE Cohesity API calls are used.

## 9. Incident prompt behavior

First run in a new Dynatrace 18:00 ET window asks:

```text
Enter incident number for this window:
```

Second run in the same window does not ask again. It reuses:

```text
X:\PowerShell\Cohesity_API_Scripts\BackupFailureWindow\Output\BackupFailure_WindowRegistry.json
```
