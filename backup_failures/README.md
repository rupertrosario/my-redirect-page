# Cohesity Backup Failure INC Status Update

## Run

```powershell
cd X:\PowerShell\Cohesity_API_Scripts\backup_failures
.\Cohesity_Backup_Failure_INC_Status_Update.ps1
```

Optional one-cluster run:

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

Temporary collector files are removed after final normalization:

```text
current_failures.csv
cleared_by_success.csv
worknotes.txt
summary.txt
```

## Do not edit generated files

Do not manually edit:

```text
incident_lifecycle.csv
worknotes_summary.txt
closing_summary.txt
state.json
```

If output looks incorrect, stale, or incomplete, rerun the script and use the refreshed files.

## incident_lifecycle.csv

Final CSV columns:

```text
Cluster,ProtectionGroup,Environment,Host,ObjectName,ObjectType,RunType,Status,OldestFailedET,NewestFailedET,LatestSuccessET,FailureRuns,Message
```

Object handling:

```text
If Cohesity did not return an object, ObjectName is blank and ObjectType is blank.
The script does not copy ProtectionGroup into ObjectName.
```

## Worknotes

`worknotes_summary.txt` has two row sections.

### Failure Section

Shows active/unresolved lifecycle rows.

Columns:

```text
Cluster | ProtectionGroup | Environment | Host | ObjectName | ObjectType | RunType | Status | OldestFailedET | NewestFailedET | LatestSuccessET | FailureRuns | Message
```

Sorted by:

```text
NewestFailedET descending
```

Team should focus on:

```text
OlderStillFailing
UnknownNeedsReview
```

### Success Section

Shows only rows newly cleared during the current check.

Included:

```text
NewlyClearedThisCheck
```

Not shown in worknotes Success Section:

```text
ClearedByLaterSuccess
```

`ClearedByLaterSuccess` remains in `incident_lifecycle.csv` and `state.json`, but is not pasted into worknotes.

Success Section columns:

```text
Cluster | ProtectionGroup | Environment | RunType | LatestSuccessET
```

The Success Section does not show object fields, host, status, old/new failure time, message, or failure count. This avoids confusion when Cohesity did not return object-level data.

## Counts

```text
Active / unresolved failures = active lifecycle rows
Newly cleared this check = NewlyClearedThisCheck rows
Previously cleared retained = ClearedByLaterSuccess rows
Total lifecycle rows = all rows in incident_lifecycle.csv
```

## Closing summary

`closing_summary.txt` uses the same Failure and Success section model and keeps:

```text
Carry Forward / Handoff
```

## Notes

- Cohesity calls are GET-only.
- No ServiceNow writes.
- No Excel output.
- No legacy CSV fallback in the operator process.
