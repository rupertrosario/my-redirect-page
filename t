# -------------------------------------------------------------
# Cohesity Oracle – Unresolved DB/Host-Level Failures (Hybrid Logic)
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

# --- Loop through each PG ---
foreach ($pg in $pgs) {

    $pgId   = $pg.id
    $pgName = $pg.name

    # --- Get runs with object details ---
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

    # sort chronologically
    $runs = $json.runs | Sort-Object { $_.localBackupInfo[0].startTimeUsecs }

    # --- Step 1: Find all failed runs ---
    foreach ($run in $runs) {
        foreach ($info in $run.localBackupInfo) {
            if ($info.status -eq "Failed") {

                $runType = $info.runType
                $runStartUsecs = $info.startTimeUsecs
                $runEndUsecs   = $info.endTimeUsecs
                $runStart = Convert-ToUtcFromEpoch $runStartUsecs
                $runEnd   = Convert-ToUtcFromEpoch $runEndUsecs

                # --- Step 2: Check for later success ---
                $laterSuccess = $runs | Where-Object {
                    $_.localBackupInfo[0].runType -eq $runType -and
                    $_.localBackupInfo[0].status -eq "Succeeded" -and
                    $_.localBackupInfo[0].startTimeUsecs -gt $runEndUsecs
                }

                if (-not $laterSuccess) {
                    # --- Step 3: Drill into objects ---
                    if ($run.objects) {
                        $dbs      = $run.objects | Where-Object { $_.object.objectType -eq 'kDatabase' }
                        $hostsObj = $run.objects | Where-Object { $_.object.environment -eq 'kPhysical' }

                        foreach ($db in $dbs) {
                            $fails = $db.localSnapshotInfo.failedAttempts
                            if ($fails -and $fails.Count -gt 0) {
                                foreach ($f in $fails) {
                                    if ($f.message) {
                                        $hostMatch = $hostsObj | Where-Object { $_.object.id -eq $db.object.sourceId } | Select-Object -First 1
                                        $hostName = if ($hostMatch) { $hostMatch.object.name } else { 'N/A' }

                                        $unresolved += [pscustomobject]@{
                                            Cluster         = $cluster_name
                                            ProtectionGroup = $pgName
                                            Hosts           = $hostName
                                            DatabaseName    = $db.object.name
                                            RunType         = $runType
                                            StartTime       = [System.TimeZoneInfo]::ConvertTimeFromUtc($runStart, $estZone)
                                            EndTime         = [System.TimeZoneInfo]::ConvertTimeFromUtc($runEnd, $estZone)
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
