# -------------------------------------------------------------
# Cohesity Oracle – Unresolved DB/Host Level Failures (Single Cluster)
# -------------------------------------------------------------

# --- Config ---
$cluster_name = "YourClusterName"
$cluster_id   = "YourClusterID"
$baseUrl      = "https://helios.cohesity.com"

$logDirectory = "X:\PowerShell\Data\Cohesity\BackupFailures"
if (-not (Test-Path $logDirectory)) { New-Item -Path $logDirectory -ItemType Directory | Out-Null }

# --- Load API key ---
$apikeypath = "X:\PowerShell\Cohesity_API_Scripts\DO_NOT_Delete\apikey.txt"
if (-not (Test-Path $apikeypath)) { throw "API key file not found at $apikeypath" }
$apiKey = (Get-Content -Path $apikeypath -Raw).Trim()

# --- Helper: Convert Epoch to UTC ---
function Convert-ToUtcFromEpoch($v){
    if($null -eq $v -or $v -eq 0){ return $null }
    try { [DateTimeOffset]::FromUnixTimeMilliseconds([int64]$v).UtcDateTime }
    catch { [DateTimeOffset]::FromUnixTimeMilliseconds([int64]([double]$v / 1000)).UtcDateTime }
}
$estZone = [System.TimeZoneInfo]::FindSystemTimeZoneById("Eastern Standard Time")

# --- API headers ---
$headers = @{
    "apiKey"          = $apiKey
    "accessClusterId" = "$cluster_id"
}

# --- Get Oracle Protection Groups ---
$pgUrl  = "$baseUrl/v2/data-protect/protection-groups"
$pgBody = @{ environments = "kOracle"; isDeleted = "False"; isPaused = "False"; isActive = "True" }
$pgResp = Invoke-WebRequest -Method Get -Uri $pgUrl -Headers $headers -Body $pgBody
$pgs    = ($pgResp.Content | ConvertFrom-Json).protectionGroups

$unresolved = @()

# --- Loop through each Protection Group ---
foreach ($pg in $pgs) {

    $pgId   = $pg.id
    $pgName = $pg.name

    # --- Get recent runs (with object details) ---
    $runUrl = "$baseUrl/v2/data-protect/protection-groups/$pgId/runs"
    $runBody = @{
        environments             = "kOracle"
        isDeleted                = "False"
        isPaused                 = "False"
        isActive                 = "True"
        numRuns                  = "20"
        excludeNonRestorableRuns = "False"
        includeObjectDetails     = "True"
    }
    $runResp = Invoke-WebRequest -Method Get -Uri $runUrl -Headers $headers -Body $runBody
    $json    = $runResp | ConvertFrom-Json
    if (-not $json.runs) { continue }

    # Sort runs chronologically by start time for accurate lookahead
    $runs = $json.runs | Sort-Object { $_.localBackupInfo[0].startTimeUsecs }

    # --- Loop through each run ---
    foreach ($run in $runs) {
        $runType = $run.localBackupInfo[0].runType
        $runStartUsecs = $run.localBackupInfo[0].startTimeUsecs
        $runEndUsecs   = $run.localBackupInfo[0].endTimeUsecs
        $runStart      = Convert-ToUtcFromEpoch $runStartUsecs
        $runEnd        = Convert-ToUtcFromEpoch $runEndUsecs

        if ($run.objects) {
            $dbObjs = $run.objects | Where-Object { $_.object.objectType -eq 'kDatabase' }

            foreach ($db in $dbObjs) {
                $fails = $db.localSnapshotInfo.failedAttempts
                if ($fails -and $fails.Count -gt 0) {

                    foreach ($f in $fails) {
                        if ($f.message) {
                            $dbId = $db.object.id
                            $hostsMatch = $run.objects | Where-Object { $_.object.id -eq $db.object.sourceId -and $_.object.environment -eq 'kPhysical' } | Select-Object -First 1
                            $hostId   = if ($hostsMatch) { $hostsMatch.object.id } else { $null }
                            $hostName = if ($hostsMatch) { $hostsMatch.object.name } else { 'N/A' }

                            # --- Check for any LATER success for this DB or host ---
                            $laterSuccess = $false
                            foreach ($r2 in $runs) {
                                if ($r2.localBackupInfo[0].startTimeUsecs -le $runEndUsecs) { continue }  # skip earlier runs
                                if ($r2.objects) {
                                    foreach ($o2 in $r2.objects) {
                                        if (
                                            ($o2.object.id -eq $dbId -or $o2.object.id -eq $hostId) -and
                                            $o2.localSnapshotInfo.status -eq "kSuccess"
                                        ) {
                                            $laterSuccess = $true
                                            break
                                        }
                                    }
                                }
                                if ($laterSuccess) { break }
                            }

                            # --- Only record if no later success exists ---
                            if (-not $laterSuccess) {
                                $unresolved += [pscustomobject]@{
                                    Cluster         = $cluster_name
                                    ProtectionGroup = $pgName
                                    Hosts           = $hostName
                                    DatabaseName    = $db.object.name
                                    RunType         = $runType
                                    StartTime       = if ($runStart) { [System.TimeZoneInfo]::ConvertTimeFromUtc($runStart, $estZone) } else { $null }
                                    EndTime         = if ($runEnd)   { [System.TimeZoneInfo]::ConvertTimeFromUtc($runEnd, $estZone) } else { $null }
                                    FailedMessage   = $f.message
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

# --- Output unresolved failures ---
if ($unresolved.Count -gt 0) {
    Write-Host "`n🔥 Unresolved DB/Host Failures ---`n"
    $unresolved | Sort-Object Cluster, ProtectionGroup, Hosts, DatabaseName |
        Format-Table Cluster, ProtectionGroup, Hosts, DatabaseName, RunType, StartTime, EndTime, FailedMessage -AutoSize

    $reportDate = Get-Date -Format "yyyy-MM-dd_HHmm"
    $csvFile = "$logDirectory\Cohesity_Unresolved_DBHost_Failures_$reportDate.csv"
    $unresolved | Export-Csv -Path $csvFile -NoTypeInformation -Encoding UTF8
    Write-Host "`n✅ Saved report to $csvFile"
}
else {
    Write-Host "`n✅ No unresolved DB/Host failures found on cluster $cluster_name."
}
