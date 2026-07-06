# Claude Guardrails — backup_failures

## Scope

This folder owns Cohesity backup-failure reporting and incident-window consolidation.

Use these files for the new consolidator:

```text
backup_failures/compute_window.js
backup_failures/Get-CohesityBackupFailureWindowConsolidator.ps1
backup_failures/window_consolidator/Get-CohesityBackupFailureWindowConsolidator.ps1
backup_failures/README_window_consolidator.md
```

Use the existing backup-failure script as style/reference only:

```text
backup_failures/Cohesity_Backup_Failures
```

## Hard Rules

1. Do not use Excel or XLSX.
2. Do not add ImportExcel, COM Excel, workbook tabs, or `.xlsx` output.
3. Output evidence as CSV, TXT, and JSON only.
4. Cohesity API behavior must remain GET-only.
5. Do not add POST, PUT, PATCH, or DELETE calls to Cohesity.
6. Do not write to ServiceNow from this script.
7. Do not ask for the incident number more than once in the same compute window.
8. Use `backup_failures/compute_window.js` as the compute-window source of truth.
9. Keep the window aligned to 18:00 ET -> next-day 18:00 ET.
10. Do not mix state between incident windows.

## Operator Validation

```powershell
cd .\backup_failures
.\Get-CohesityBackupFailureWindowConsolidator.ps1 `
  -MaxClusters 1 `
  -MaxProtectionGroupsPerCluster 3 `
  -NoGridView
```

Expected result:

```text
Prompts once for the incident on first run.
Reuses the same incident on later runs in the same window.
Creates CSV/TXT/JSON only.
Creates no XLSX.
```
