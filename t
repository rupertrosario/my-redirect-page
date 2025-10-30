# -------------------------------------------------------------
# Cohesity Oracle – Unresolved DB/Host Failures
# (Run-first → no-later-success → object drill with progressTaskId-matched messages)
# -------------------------------------------------------------

# --- Config ---
$cluster_name = "YourClusterName"
$cluster_id   = "YourClusterID"
$baseUrl      = "https://helios.cohesity.com"
$apiKeyPath   = "X:\PowerShell\Cohesity_API_Scripts\DO_NOT_Delete\apikey.txt"

$ErrorActionPreference = 'Stop'

if (-not (Test-Path $apiKeyPath)) { throw "API key file not found: $apiKeyPath" }
$apiKey = (Get-Content -Path $apiKeyPath -Raw).Trim()
$headers = @{ apiKey = $apiKey; accessClusterId = $cluster_id }

function Convert-ToUtcFromEpoch($v){
    if ($null -eq $v -or $v -eq 0) { return $null }
    try { [DateTimeOffset]::FromUnixTimeMilliseconds([int64]$v).UtcDateTime }
    catch { [DateTimeOffset]::FromUnixTimeMilliseconds([int64]([double]$v / 1000)).UtcDateTime }
}
$tz = [System.TimeZoneInfo]::FindSystemTimeZoneById("Eastern Standard Time")

# --- Get Protection Groups (Oracle, active) ---
$pgResp = Invoke-WebRequest -Uri "$baseUrl/v2/data-protect/protection-groups" -Headers $headers -Body @{
    environments = "kOracle"; isDeleted = "False"; isPaused = "False"; isActive = "True"
} -Method Get
$pgs = ($pgResp.Content | ConvertFrom-Json).protectionGroups

$rows = @()

foreach ($pg in $pgs) {
    $pgId   = $pg.id
    $pgName = $pg.name

    # --- Get latest 10 runs with object details ---
    $runResp = Invoke-WebRequest -Uri "$baseUrl/v2/data-protect/protection-groups/$pgId/runs" -Headers $headers -Body @{
        environments             = "kOracle"
        isDeleted                = "False"
        isPaused                 = "False"
        isActive                 = "True"
        numRuns                  = "10"
        excludeNonRestorableRuns = "False"
        includeObjectDetails     = "True"
    } -Method Get

    $json = $runResp | ConvertFrom-Json
    if (-not $json.runs) { continue }
    $runs = $json.runs | Sort-Object { $_.localBackupInfo[0].startTimeUsecs }  # chronological

    # --- Step 1: find failed runs ---
    foreach ($run in $runs) {
        foreach ($info in $run.localBackupInfo) {
            if ($info.status -ne "Failed") { continue }

            $runType     = $info.runType
            $runStartUs  = [int64]$info.startTimeUsecs
            $runEndUs    = [int64]$info.endTimeUsecs
            $progressId  = $info.progressTaskId

            # --- Step 2: later success of same runType? skip if found ---
            $laterSuccess = $runs | Where-Object {
                $_.localBackupInfo[0].runType -eq $runType -and
                $_.localBackupInfo[0].status  -eq "Succeeded" -and
                [int64]$_.localBackupInfo[0].startTimeUsecs -gt $runEndUs
            }
            if ($laterSuccess) { continue }

            # --- Step 3: drill into this failed run, DB + host, progressTaskId-matched messages only ---
            if (-not $run.objects) { continue }

            $dbObjs    = $run.objects | Where-Object { $_.object.objectType  -eq 'kDatabase' }
            $hostsObjs = $run.objects | Where-Object { $_.object.environment -eq 'kPhysical' }

            $startLocal = [System.TimeZoneInfo]::ConvertTimeFromUtc((Convert-ToUtcFromEpoch $runStartUs), $tz)
            $endLocal   = [System.TimeZoneInfo]::ConvertTimeFromUtc((Convert-ToUtcFromEpoch $runEndUs),   $tz)

            foreach ($db in $dbObjs) {
                $attempts = $db.localSnapshotInfo.failedAttempts
                if (-not $attempts) { continue }

                # STRICT: only messages whose progressTaskId == run's progressTaskId
                $matched = @($attempts | Where-Object { $_.progressTaskId -and $_.progressTaskId -eq $progressId -and $_.message })
                if ($matched.Count -eq 0) { continue }  # no task-id aligned messages → skip this DB

                $hostObj  = $hostsObjs | Where-Object { $_.object.id -eq $db.object.sourceId } | Select-Object -First 1
                $hostName = if ($hostObj) { $hostObj.object.name } else { 'N/A' }

                foreach ($fa in $matched) {
                    $rows += [pscustomobject]@{
                        Cluster         = $cluster_name
                        ProtectionGroup = $pgName
                        Hosts           = $hostName
                        DatabaseName    = $db.object.name
                        RunType         = $runType
                        StartTime       = $startLocal
                        EndTime         = $endLocal
                        FailedMessage   = $fa.message
                        ProgressTaskId  = $progressId
                    }
                }
            }
        }
    }
}

# --- Output ---
if ($rows.Count -gt 0) {
    Write-Host "`n🔥 Unresolved failures (messages matched by progressTaskId):`n"
    $rows | Sort-Object ProtectionGroup, Hosts, DatabaseName |
      Format-Table ProtectionGroup, Hosts, DatabaseName, RunType, StartTime, EndTime, FailedMessage, ProgressTaskId -AutoSize
} else {
    Write-Host "`n✅ No unresolved DB/Host failures with matching progressTaskId found."
}
