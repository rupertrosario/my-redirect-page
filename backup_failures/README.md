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

## First test

```powershell
cd backup_failures
.\Get-CohesityBackupFailureWindowConsolidator.ps1 `
  -MaxClusters 1 `
  -MaxProtectionGroupsPerCluster 3 `
  -ShowGridView:$false
```

Run it twice. First run should ask for incident. Second run in the same DT window should not ask again.
