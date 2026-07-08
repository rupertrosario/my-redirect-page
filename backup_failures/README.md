# Cohesity Backup Failure INC Status Update

## Run

```powershell
cd X:\PowerShell\Cohesity_API_Scripts\backup_failures
.\Cohesity_Backup_Failure_INC_Status_Update.ps1
```

One cluster test:

```powershell
.\Cohesity_Backup_Failure_INC_Status_Update.ps1 -ClusterName "YOUR_CLUSTER_NAME"
```

Optional grid view:

```powershell
.\Cohesity_Backup_Failure_INC_Status_Update.ps1 -ShowGrid
```

Optional shorter request timeout per Cohesity call:

```powershell
.\Cohesity_Backup_Failure_INC_Status_Update.ps1 -ClusterName "YOUR_CLUSTER_NAME" -RequestTimeoutSec 45
```

## Script responsibility

Main collection logic is in:

```text
Get-CohesityBackupFailureWindowConsolidator.ps1
```

The wrapper only calls the main collector and prints final file paths:

```text
Cohesity_Backup_Failure_INC_Status_Update.ps1
```

## Final operator-facing files

```text
worknotes_summary.txt
incident_lifecycle.csv
closing_summary.txt
```

`state.json` is script memory. Do not manually edit it and do not attach it to the incident.

## Current behavior

The worknotes are intentionally simple:

```text
Failure Section
Success Section
```

The main collector refreshes the failure list at object level using the protection group runs endpoint with:

```text
includeObjectDetails=true
```

For object-level failures, the script checks the same object identity across later runs. If the object has a later successful backup, it should not remain as an active failure.

## incident_lifecycle.csv

Final CSV columns:

```text
Cluster,ProtectionGroup,Environment,Host,ObjectName,ObjectType,RunType,Status,OldestFailedET,NewestFailedET,LatestSuccessET,FailureRuns,Message
```

Object handling:

```text
ObjectName and ObjectType are populated only when Cohesity returns object evidence.
The script does not copy ProtectionGroup into ObjectName.
```

## Worknotes sections

### Failure Section

Columns:

```text
Cluster | ProtectionGroup | Environment | Host | ObjectName | ObjectType | RunType | Status | OldestFailedET | NewestFailedET | LatestSuccessET | FailureRuns | Message
```

### Success Section

Columns:

```text
Cluster | ProtectionGroup | Environment | RunType | LatestSuccessET
```

Only `NewlyClearedThisCheck` rows are shown in the Success Section.

## Notes

- Cohesity calls are GET-only.
- No ServiceNow writes.
- No Excel output.
- No legacy CSV fallback in the operator process.
