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

## Final operator-facing files

```text
worknotes_summary.txt
incident_lifecycle.csv
closing_summary.txt
```

`state.json` is script memory. Do not manually edit it and do not attach it to the incident.

## Main output model

The operator output is now split by evidence level.

```text
Object-Level Failure Section
Object-Level Success Section
Run-Level / PG-Level Review Section
Run-Level / PG-Level Success Section
```

Use the object-level sections for normal troubleshooting.

Run-level / PG-level rows mean Cohesity reported the run or protection group as failed, but did not return object-level failedAttempts for that row.

## Simple Result values

Operator-facing result values are simplified:

```text
Failed
Running
Cancelled
Success
```

Internal lifecycle status is still retained in `incident_lifecycle.csv` and `state.json` for script tracking.

## incident_lifecycle.csv

Final CSV columns:

```text
Cluster,ProtectionGroup,Environment,Host,ObjectName,ObjectType,RunType,Result,EvidenceLevel,Status,OldestFailedET,NewestFailedET,LatestSuccessET,FailureRuns,Message
```

Important fields:

```text
Result        = Failed, Running, Cancelled, or Success
EvidenceLevel = Object or RunLevel
Status        = internal lifecycle state retained for script tracking
```

Object handling:

```text
Object-level row: ObjectName and ObjectType are populated.
Run-level row: ObjectName and ObjectType are blank in final normalized output.
The script does not copy ProtectionGroup into ObjectName.
```

## Object-level rule

For object-level troubleshooting, check by object:

```text
Same cluster + protection group + environment + run type + object identity
Later successful backup found
= object should not remain in the Object-Level Failure Section
```

Rows without object evidence are not mixed into object-level failures. They are moved to the Run-Level / PG-Level Review Section.

## Worknotes sections

### Object-Level Failure Section

Columns:

```text
Cluster | ProtectionGroup | Environment | Host | ObjectName | ObjectType | RunType | Result | NewestFailedET | FailureRuns | Message
```

### Object-Level Success Section

Columns:

```text
Cluster | ProtectionGroup | Environment | Host | ObjectName | ObjectType | RunType | Result | LatestSuccessET
```

### Run-Level / PG-Level Review Section

Columns:

```text
Cluster | ProtectionGroup | Environment | RunType | Result | NewestFailedET | LatestSuccessET | Message
```

## Notes

- Cohesity calls are GET-only.
- No ServiceNow writes.
- No Excel output.
- No legacy CSV fallback in the operator process.
