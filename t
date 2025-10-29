$flatRuns = @()

# 1️⃣ Flatten all runs into one array
foreach ($run in $runs) {
    if ($run.localBackupInfo) {
        foreach ($info in $run.localBackupInfo) {
            $flatRuns += [pscustomobject]@{
                Cluster         = $clusterName
                ProtectionGroup = $pgName
                RunType         = $info.runType
                Status          = $info.status
                Message         = $info.message
                StartTimeUsecs  = $info.startTimeUsecs
                EndTimeUsecs    = $info.endTimeUsecs
            }
        }
    }
}

# 2️⃣ Split successes vs failures
$latestFailures = $flatRuns | Where-Object { $_.Status -notin @("Succeeded","SucceededWithWarning") } | Sort-Object -Property EndTimeUsecs -Descending
$successes      = $flatRuns | Where-Object { $_.Status -in @("Succeeded","SucceededWithWarning") }

# 3️⃣ Filter only those failures that have no later success
$allFailures = @()
foreach ($fail in $latestFailures) {
    $hasLaterSuccess = $successes | Where-Object {
        $_.RunType -eq $fail.RunType -and $_.StartTimeUsecs -gt $fail.EndTimeUsecs
    }

    if (-not $hasLaterSuccess) {
        # convert times nicely
        $startUtc = [datetime]"1970-01-01".AddMilliseconds($fail.StartTimeUsecs / 1000)
        $endUtc   = [datetime]"1970-01-01".AddMilliseconds($fail.EndTimeUsecs / 1000)
        $estZone  = [System.TimeZoneInfo]::FindSystemTimeZoneById("Eastern Standard Time")

        $allFailures += [pscustomobject]@{
            Cluster         = $fail.Cluster
            ProtectionGroup = $fail.ProtectionGroup
            RunType         = $fail.RunType
            Status          = $fail.Status
            Message         = $fail.Message
            StartTime       = [System.TimeZoneInfo]::ConvertTimeFromUtc($startUtc, $estZone).ToString("dd/MM/yyyy HH:mm:ss")
            EndTime         = [System.TimeZoneInfo]::ConvertTimeFromUtc($endUtc, $estZone).ToString("dd/MM/yyyy HH:mm:ss")
        }
    }
}

# 4️⃣ Display or export
$allFailures | Sort-Object Cluster, ProtectionGroup, RunType
# $allFailures | Export-Csv "LatestFailures.csv" -NoTypeInformation
