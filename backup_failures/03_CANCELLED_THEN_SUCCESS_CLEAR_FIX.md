# Cancelled then later success - consolidator fix

Use this when a backup/object is cancelled first and then later succeeds.

## Rule

```text
If the same ObjectKey has older Failure / Running / Cancelled evidence and the latest object/snapshot state is Success, the object is recovered.
```

This means it must be written to:

```text
cleared_by_success.csv
```

and counted in:

```text
Recovered Today
Success Section
```

It must **not** remain in:

```text
Active Cancelled
Cancelled Backup PGs
Failure Section
```

## Where the fix belongs

The fix belongs in:

```text
backup_failures/Get-CohesityBackupFailureWindowConsolidator.ps1
```

inside:

```powershell
function Process-DetailedRuns
```

## Exact logic change

The current success block must not require only `$PreviousRow`.
It must also clear when the current scan itself found older failed/running/cancelled evidence:

```powershell
if ($Entry.LatestState -eq 'Success') {
    # Recovery-aware rule:
    # If the latest object/snapshot state is Success, any older Failure/Running/Cancelled
    # evidence in the same scan is cleared by that newer success. This covers cases such as
    # a cancelled backup followed by a later successful backup before the next saved state exists.
    if ($PreviousRow -or [int]$Entry.FailedRunCount -gt 0) {
        $ClearMessage = 'Previously failed/running/cancelled object has newer successful backup.'
        if (!$PreviousRow -and [int]$Entry.FailedRunCount -gt 0) {
            $ClearMessage = 'Earlier failed/running/cancelled object state in this scan has newer successful backup.'
        }
        $ClearedRows += New-StatusRow -Incident $Incident -WindowKey $WindowKey -Status 'Success' -Change 'Cleared' -ClusterDisplayName $ClusterDisplayName -ClusterId $ClusterId -EnvironmentSpec $EnvironmentSpec -ProtectionGroupName $ProtectionGroupName -ProtectionGroupId $ProtectionGroupId -ParentHostName $Entry.ParentHostName -ObjectName $Entry.ObjectName -ObjectType $Entry.ObjectType -RunType $Entry.RunType -FirstFailedUsecs 0 -LastFailedUsecs 0 -LatestSuccessUsecs $Entry.LatestSuccessUsecs -LastSeenUsecs $Entry.LatestUsecs -FailureDates $MergedFailureDates -FailedRunCount $Entry.FailedRunCount -Message $ClearMessage -ObjectKey $ObjectKey -EnvironmentFilter $EnvironmentSpec.Filter -FailedRunKeys (($Entry.FailedRunKeys | Select-Object -Unique) -join ' | ')
    }
    continue
}
```

## Replacement file prepared

Use the V3 replacement file from ChatGPT:

```text
Get-CohesityBackupFailureWindowConsolidator_ENV_FIRST_FIXED_V3.ps1
```

Expected SHA256:

```text
D66072368B2D218595790A9A7CC90D9EF6ECEEB9C993DAA177D975B43A12F992
```

## Verify after replacing

```powershell
cd X:\PowerShell\Cohesity_API_Scripts\backup_failures

Select-String .\Get-CohesityBackupFailureWindowConsolidator.ps1 -Pattern `
"Recovery-aware rule","cancelled backup followed by a later successful backup","Processing clusters alphabetically"
```

Expected:

```text
Recovery-aware rule                                      = match
cancelled backup followed by a later successful backup   = match
Processing clusters alphabetically                       = no match
```
