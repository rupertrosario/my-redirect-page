# --- Assume $runs already fetched from:
# GET https://helios.cohesity.com/v2/data-protect/protection-groups/$pgId/runs

$flatRuns = @()

foreach ($run in $runs) {
    # Each $run can contain multiple objects of various types
    foreach ($obj in $run.objects) {

        # Only process database objects
        if ($obj.object.objectType -eq "kDatabase") {

            # Each DB object has localBackupInfo[] for its backup cycles
            foreach ($info in $obj.localBackupInfo) {
                $flatRuns += [pscustomobject]@{
                    RunType         = $info.runType
                    Status          = $info.status
                    Object          = $obj.object.name
                    ObjectType      = $obj.object.objectType -replace '^k',''
                    Message         = $info.messages
                    StartTimeUsecs  = $info.stats.startTimeUsecs
                    EndTimeUsecs    = $info.stats.endTimeUsecs
                    Cluster         = $cluster_name
                    ProtectionGroup = $pgName
                }
            }
        }
    }
}

# Optional: pretty print
$flatRuns | Format-Table -AutoSize