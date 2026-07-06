# Cohesity Backup Failure Window Consolidator

## Decision

Use this tool for backup-failure incident evidence and work-note consolidation.

It is not a replacement for Cohesity UI. It is an operational consolidator for one Dynatrace incident window.

## Source of Truth

Window source: `compute_window.js`

Current window definition:

```text
Daily 18:00 ET -> next-day 18:00 ET
Window key format: yyyy-MM-dd_1800ET
Correlation id: Cohesity_Backup_Failures
```

The PowerShell script intentionally uses the same window model so Dynatrace incident creation and operator evidence stay synchronized.

## Incident Locking

One incident owns one compute window.

```text
IncidentNumber + WindowKey + WindowStartET + WindowEndET = locked scope
```

Behavior:

- First run in a new window prompts for the incident number.
- Later runs in the same window reuse the registry lock and do not ask again.
- A new compute window prompts for the new incident.
- Existing window mappings are not overwritten accidentally.

Registry:

```text
X:\PowerShell\Data\Cohesity\BackupFailureWindow\BackupFailure_WindowRegistry.json
```

Incident state:

```text
X:\PowerShell\Data\Cohesity\BackupFailureWindow\<INCNUMBER>\<INCNUMBER>_State.json
```

## Output Type

CSV/TXT/JSON only.

No Excel workbook is generated.

## Files Created

For each incident, output is written to:

```text
X:\PowerShell\Data\Cohesity\BackupFailureWindow\<INCNUMBER>\
```

Files:

```text
<INCNUMBER>_00_Run_Status.csv
<INCNUMBER>_01_Summary.csv
<INCNUMBER>_02_Current_Still_Failing.csv
<INCNUMBER>_03_Recovered_In_Window.csv
<INCNUMBER>_04_New_Failures_Latest.csv
<INCNUMBER>_05_New_Recoveries_Latest.csv
<INCNUMBER>_06_Consecutive_Failures.csv
<INCNUMBER>_07_Carry_Forward_Baseline.csv
<INCNUMBER>_08_Event_History.csv
<INCNUMBER>_09_Run_Evidence.csv
<INCNUMBER>_10_Metadata.csv
<INCNUMBER>_11_Warnings.csv
<INCNUMBER>_QuickView.csv
<INCNUMBER>_WorkNotes_Paste.txt
<INCNUMBER>_State.json
```

## Fast Test

Use this first on one cluster and a small protection-group count.

```powershell
.\Get-CohesityBackupFailureWindowConsolidator.ps1 `
  -MaxClusters 1 `
  -MaxProtectionGroupsPerCluster 3 `
  -NoGridView
```

## Specific Cluster Test

```powershell
.\Get-CohesityBackupFailureWindowConsolidator.ps1 `
  -ClusterName "CLUSTER_NAME" `
  -MaxProtectionGroupsPerCluster 5 `
  -NoGridView
```

## Normal Operator Run

```powershell
.\Get-CohesityBackupFailureWindowConsolidator.ps1
```

First run in a window asks:

```text
Enter incident number for this window:
```

Subsequent runs in the same window do not ask again.

## GET-only Behavior

The script only calls Cohesity Helios GET endpoints.

Primary endpoints:

```text
GET /v2/mcm/cluster-mgmt/info
GET /v2/data-protect/protection-groups
GET /v2/data-protect/protection-groups/{pgId}/runs
```

Headers:

```text
accept: application/json
apiKey: <from file>
accessClusterId: <cluster id when cluster scoped>
```

## Status Logic

| Status | Meaning |
|---|---|
| StillFailing | Failed in the locked window and no later success in that same window |
| RecoveredInWindow | Failed in the locked window and later succeeded in the same window |
| NewlyFailedThisCheck | Not failing in previous script state, failing now |
| NewlyRecoveredThisCheck | Was failing in previous script state, recovered now |
| ConsecutiveFailure | Multiple failures without a later success |
| ReFailed | Succeeded earlier, then failed again later |
| RunningAtLatestCheck | Running run seen inside the window |
| CancelledInWindow | Cancelled run seen inside the window |

## Operator Outcome

```text
Run PowerShell.
Review one GridView or QuickView CSV.
Attach generated CSV/TXT/JSON files.
Paste WorkNotes_Paste.txt into ServiceNow work_notes.
Troubleshoot only what remains failing.
```

## Important Note

Script success only means the script collected and consolidated the window correctly.

It does not mean backups succeeded.
